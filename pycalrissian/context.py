import base64
import json
import os
from typing import Dict, TextIO

from kubernetes import client, config
from kubernetes.client import Configuration
from kubernetes.client.models.v1_persistent_volume_claim import V1PersistentVolumeClaim
from kubernetes.client.rest import ApiException


class CalrissianContext(object):
    def __init__(
        self,
        namespace: str,
        storage_class: str,
        volume_size: str,
        image_pull_secrets: Dict = None,
        kubeconfig_file: TextIO = None,
    ):

        self.kubeconfig_file = kubeconfig_file

        self.api_client = self._get_api_client(self.kubeconfig_file)
        self.core_v1_api = self._get_core_v1_api()
        self.batch_v1_api = self._get_batch_v1_api()
        self.rbac_authorization_v1_api = self._get_rbac_authorization_v1_api()

        self.namespace = namespace
        self.storage_class = storage_class
        self.volume_size = volume_size

        self.image_pull_secrets = image_pull_secrets
        self.secret_name = "container-rg"

    def initialise(self):

        # create namespace
        if not self.is_namespace_created():
            self.create_namespace()

        # create roles and role binding
        roles = {}

        roles["pod-manager-role"] = {
            "verbs": ["create", "patch", "delete", "list", "watch"],
            "role_binding": "pod-manager-default-binding",
        }

        roles["log-reader-role"] = {
            "verbs": ["get", "list"],
            "role_binding": "log-reader-default-binding",
        }

        for key, value in roles.items():
            response = self.create_role(
                name=key,
                verbs=value["verbs"],
                resources=["pods", "pods/log"],
                api_groups=["*"],
            )
            # print(type(response))
            # assert(isinstance(response, V1Role))

            self.create_role_binding(name=value["role_binding"], role=key)
            # assert(isinstance(response, V1RoleBinding))

        # create volumes
        response = self.create_pvc(
            name="calrissian-wdir",
            size=self.volume_size,
            storage_class=self.storage_class,
            access_modes=["ReadWriteMany"],
        )

        assert isinstance(response, V1PersistentVolumeClaim)

        if self.image_pull_secrets:

            self.create_image_pull_secret(self.secret_name)

            # self.patch_service_account()

    def dispose(self):

        # TODO
        # Add delete for the pods

        try:
            response = self.core_v1_api.delete_namespace(
                name=self.namespace, pretty=True, grace_period_seconds=0
            )
            return response

        except ApiException as e:
            raise e

    def get_tmp_dir(self):
        """Returns the tmp directory path"""

    def get_output_dir(self):
        """Returns the output directory path"""

    @staticmethod
    def _get_api_client(kubeconfig_file: TextIO = None):

        proxy_url = os.getenv("HTTP_PROXY", None)
        kubeconfig = os.getenv("KUBECONFIG", None)

        if proxy_url:
            api_config = Configuration(host=proxy_url)
            api_config.proxy = proxy_url
            api_client = client.ApiClient(api_config)

        elif kubeconfig:
            # this is needed because kubernetes-python does not consider
            # the KUBECONFIG env variable
            config.load_kube_config(config_file=kubeconfig)
            api_client = client.ApiClient()
        elif kubeconfig_file:
            config.load_kube_config(config_file=kubeconfig)
            api_client = client.ApiClient()
        else:
            # if nothing is specified, kubernetes-python will use the file
            # in ~/.kube/config
            config.load_kube_config()
            api_client = client.ApiClient()

        return api_client

    def _get_core_v1_api(self) -> client.CoreV1Api:

        return client.CoreV1Api(api_client=self.api_client)

    def _get_batch_v1_api(self) -> client.BatchV1Api:

        return client.BatchV1Api(api_client=self.api_client)

    def _get_rbac_authorization_v1_api(self) -> client.RbacAuthorizationApi:

        return client.RbacAuthorizationV1Api(self.api_client)

    def is_object_created(self, read_method, **kwargs):

        read_methods = {}

        read_methods["read_namespace"] = self.core_v1_api.read_namespace
        read_methods[
            "read_namespaced_role"
        ] = self.rbac_authorization_v1_api.read_namespaced_role  # noqa: E501
        read_methods[
            "read_namespaced_role_binding"
        ] = self.rbac_authorization_v1_api.read_namespaced_role_binding  # noqa: E501

        read_methods[
            "read_namespaced_config_map"
        ] = self.core_v1_api.read_namespaced_config_map  # noqa: E501

        read_methods[
            "read_namespaced_persistent_volume_claim"
        ] = self.core_v1_api.read_namespaced_persistent_volume_claim  # noqa: E501

        read_methods[
            "read_namespaced_secret"
        ] = self.core_v1_api.read_namespaced_secret  # noqa: E501

        created = False

        try:

            if read_method in [
                "read_namespaced_config_map",
                "read_namespaced_role",
                "read_namespaced_role_binding",
                "read_namespaced_persistent_volume_claim",
                "read_namespaced_secret",
            ]:
                read_methods[read_method](namespace=self.namespace, **kwargs)
                created = True
            else:
                read_methods[read_method](self.namespace)
                created = True
        except ApiException as e:
            print(e.status)
            if e.status == 404:
                created = False

        return created

    def is_namespace_created(self, **kwargs) -> bool:

        return self.is_object_created("read_namespace", **kwargs)

    def is_role_binding_created(self, **kwargs) -> bool:

        return self.is_object_created("read_namespaced_role_binding", **kwargs)

    def is_role_created(self, **kwargs) -> bool:

        return self.is_object_created("read_namespaced_role", **kwargs)

    def is_config_map_created(self, **kwargs) -> bool:

        return self.is_object_created("read_namespaced_config_map", **kwargs)

    def is_pvc_created(self, **kwargs) -> bool:

        return self.is_object_created(
            "read_namespaced_persistent_volume_claim", **kwargs
        )  # noqa: E501

    def is_image_pull_secret_created(self, **kwargs) -> bool:

        return self.is_object_created("read_namespaced_secret", **kwargs)

    def create_namespace(self, job_labels: dict = None) -> client.V1Namespace:

        if self.is_namespace_created():

            return self.core_v1_api.read_namespace(name=self.namespace)

        else:

            try:
                body = client.V1Namespace(
                    metadata=client.V1ObjectMeta(
                        name=self.namespace, labels=job_labels
                    )  # noqa: E501
                )
                response = self.core_v1_api.create_namespace(
                    body=body, async_req=False
                )  # noqa: E501
                return response
            except ApiException as e:
                raise e

    def create_role(
        self,
        name: str,
        verbs: list,
        resources: list = ["pods", "pods/log"],
        api_groups: list = ["*"],
    ):

        if self.is_role_created(name=name):

            return self.rbac_authorization_v1_api.read_namespaced_role(
                name=name, namespace=self.namespace
            )

        else:
            metadata = client.V1ObjectMeta(name=name, namespace=self.namespace)

            rule = client.V1PolicyRule(
                api_groups=api_groups,
                resources=resources,
                verbs=verbs,
            )

            body = client.V1Role(metadata=metadata, rules=[rule])

            try:
                response = (
                    self.rbac_authorization_v1_api.create_namespaced_role(  # noqa: E501
                        self.namespace, body, pretty=True
                    )
                )
                return response

            except ApiException as e:
                raise e

    def create_role_binding(self, name: str, role: str):

        if self.is_role_binding_created(name=name):

            return self.rbac_authorization_v1_api.read_namespaced_role_binding(
                name=name, namespace=self.namespace
            )

        else:

            metadata = client.V1ObjectMeta(name=name, namespace=self.namespace)

            role_ref = client.V1RoleRef(api_group="", kind="Role", name=role)

            subject = client.models.V1Subject(
                api_group="",
                kind="ServiceAccount",
                name="default",
                namespace=self.namespace,
            )

            body = client.V1RoleBinding(
                metadata=metadata, role_ref=role_ref, subjects=[subject]
            )  # noqa: E501

            try:
                response = self.rbac_authorization_v1_api.create_namespaced_role_binding(  # noqa: E501
                    self.namespace, body, pretty=True
                )
                return response
            except ApiException as e:

                raise e

    def create_pvc(
        self,
        name,
        access_modes,
        size,
        storage_class,
    ):

        if self.is_pvc_created(name=name):

            return self.core_v1_api.read_namespaced_persistent_volume_claim(
                name=name, namespace=self.namespace
            )

        else:

            metadata = client.V1ObjectMeta(name=name, namespace=self.namespace)

            spec = client.V1PersistentVolumeClaimSpec(
                access_modes=access_modes,
                resources=client.V1ResourceRequirements(
                    requests={"storage": size}
                ),  # noqa: E501
            )

            spec.storage_class_name = storage_class

            body = client.V1PersistentVolumeClaim(metadata=metadata, spec=spec)

            try:
                response = self.core_v1_api.create_namespaced_persistent_volume_claim(  # noqa: E501
                    self.namespace, body, pretty=True
                )
                return response
            except ApiException as e:

                raise e

    # def dispose(self) -> client.V1Status:
    #     try:

    #         response = self.core_v1_api.delete_namespace(
    #             name=self.namespace, pretty=True, grace_period_seconds=0
    #         )
    #         return response

    #     except ApiException as e:
    #         raise e

    def create_configmap(
        self,
        name,
        key,
        content,
        annotations: Dict = {},
        labels: Dict = {},
    ):

        if self.is_config_map_created(name=name):

            return self.core_v1_api.read_namespaced_config_map(
                namespace=self.namespace, name=name
            )  # noqa: E501

        else:

            metadata = client.V1ObjectMeta(
                annotations=annotations,
                deletion_grace_period_seconds=30,
                labels=labels,
                name=name,
                namespace=self.namespace,
            )

            data = {}
            data[key] = content

            config_map = client.V1ConfigMap(
                api_version="v1",
                kind="ConfigMap",
                data=data,
                metadata=metadata,
            )

            try:
                response = self.core_v1_api.create_namespaced_config_map(
                    namespace=self.namespace,
                    body=config_map,
                    pretty=True,
                )
                return response

            except ApiException as e:
                raise e

    def create_image_pull_secret(
        self,
        name,
    ):

        if self.is_image_pull_secret_created(name=name):

            return self.core_v1_api.read_namespaced_secret(
                namespace=self.namespace, name=name
            )  # noqa: E501

        else:

            metadata = {"name": name, "namespace": self.namespace}

            data = {
                ".dockerconfigjson": base64.b64encode(
                    json.dumps(self.image_pull_secrets).encode()
                ).decode()
            }

            secret = client.V1Secret(
                api_version="v1",
                data=data,
                kind="Secret",
                metadata=metadata,
                type="kubernetes.io/dockerconfigjson",
            )

            try:
                response = self.core_v1_api.create_namespaced_secret(
                    namespace=self.namespace,
                    body=secret,
                    pretty=True,
                )
                return response

            except ApiException as e:
                raise e

    def patch_service_account(self):
        # adds a secret to the namespace default service account

        service_account_body = self.core_v1_api.read_namespaced_service_account(
            name="default", namespace=self.namespace
        )

        if service_account_body.secrets is None:
            service_account_body.secrets = []

        if service_account_body.image_pull_secrets is None:
            service_account_body.image_pull_secrets = []

        service_account_body.secrets.append({"name": self.secret_name})
        service_account_body.image_pull_secrets.append(
            {"name": self.secret_name}
        )  # noqa: E501

        try:
            self.core_v1_api.patch_namespaced_service_account(
                name="default",
                namespace=self.namespace,
                body=service_account_body,
                pretty=True,
            )
        except ApiException as e:
            raise e

    # def create_roles(self):

    #     roles = {}

    #     roles["pod-manager-role"] = {
    #         "verbs": ["create", "patch", "delete", "list", "watch"],
    #         "role_binding": "pod-manager-default-binding",
    #     }

    #     roles["log-reader-role"] = {
    #         "verbs": ["get", "list"],
    #         "role_binding": "log-reader-default-binding",
    #     }

    #     for role, value in roles.items():

    #         self.create_role(
    #             name=role,
    #             verbs=value["verbs"],
    #             resources=["pods", "pods/log"],
    #             api_groups=["*"],
    #         )
    #         self.create_role_binding(name=value["role_binding"], role=role)