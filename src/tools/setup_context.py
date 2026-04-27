"""SetupContext: all TPC-DI bootstrap defaults and helpers in one object.

Replaces the flat variable-leakage pattern of the legacy ``setup`` notebook.
Import and instantiate once per session:

    from setup_context import SetupContext
    ctx = SetupContext(spark, dbutils)

    ctx.api_call(...)
    ctx.default_catalog
    ctx.node_types
    ...

The legacy ``setup`` notebook is now a thin shim that creates ``ctx`` and
re-exports its attributes as flat names for backward compatibility until
callers finish migrating to ``ctx.X``.
"""
import collections
import json
import string

import requests


class SetupContext:
    """Bootstrap context — API auth, cloud detection, node/DBR catalogs, defaults."""

    # Static catalogs (don't need cluster info to populate)
    workflows_dict = {
        "CLUSTER":     "Workspace Cluster Workflow",
        "DBSQL":       "DBSQL Warehouse Workflow",
        "SDP-CORE":    "CORE Spark Declarative Pipelines",
        "SDP-PRO":     "PRO Spark Declarative Pipelines with SCD Type 1/2",
        "SDP-ADVANCED":"ADVANCED Spark Declarative Pipelines with DQ",
    }
    min_dbr_version = 14.1
    invalid_dbr_list = [
        "aarch64", "ML", "Snapshot", "GPU", "Photon",
        "RC", "Light", "HLS", "Beta", "Latest",
    ]
    features_or_perf = ["Feature-Rich", "Fastest Performance"]
    default_sf = "10"
    default_sf_options = ["10", "100", "1000", "5000", "10000", "20000"]
    default_serverless = "YES"

    def __init__(self, spark, dbutils):
        self.spark = spark
        self.dbutils = dbutils
        self.worker_cores_mult = 0.016  # instance-scoped so GCP can bump it

        self._init_api_context()
        self._init_workflow_defaults()
        self._init_cloud_defaults()
        self._init_compute_and_catalog_defaults()

    # ---------- API + workspace paths ----------

    def _init_api_context(self):
        ctx = self.dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        self.api_url = ctx.apiUrl().getOrElse(None)
        self.token = ctx.apiToken().getOrElse(None)
        if self.api_url is None or self.token is None:
            self.dbutils.notebook.exit(
                "Unable to capture API/Token from dbutils. Please try again or open a ticket")
        nb_path = ctx.notebookPath().get()
        self.repo_src_path = f"{nb_path.split('/src')[0]}/src"
        self.workspace_src_path = f"/Workspace{self.repo_src_path}"
        self.user_name = self.spark.sql(
            "select lower(regexp_replace(split(current_user(), '@')[0], '(\\\\W+)', ' '))"
        ).collect()[0][0]

    def api_call(self, json_payload=None, request_type=None, api_endpoint=None):
        headers = {
            "Content-type": "application/json",
            "Accept": "*/*",
            "Authorization": f"Bearer {self.token}",
        }
        url = f"{self.api_url}{api_endpoint}"
        if request_type == "POST":
            response = requests.post(url, json=json_payload, headers=headers)
        elif request_type == "GET":
            response = requests.get(url, json=json_payload, headers=headers)
        else:
            self.dbutils.notebook.exit(f"Invalid request type: {request_type}")
            return
        if response.status_code == 200:
            return response
        self.dbutils.notebook.exit(
            f"API call failed with status code {response.status_code}: {response.text}")

    # ---------- Catalog/node/DBR lookups ----------

    def get_node_types(self):
        """Return full Databricks /list-node-types catalog as
        ``{node_type_id: {full_info_dict}}``.

        The widget consumer uses ``list(self.node_types.keys())`` for the
        dropdown options. The workflow_builders use the full info_dict
        (num_cores, category, num_local_disks, etc.) via
        ``workflow_builders._node_picker.pick_node()`` to choose the best
        ARM-with-local-disk node for data-gen clusters per cloud.
        """
        response = self.api_call(None, "GET", "/api/2.0/clusters/list-node-types")
        items = json.loads(response.text)["node_types"]
        out = {}
        for node in items:
            nid = node.pop("node_type_id")
            out[nid] = node
        return collections.OrderedDict(sorted(out.items()))

    def get_dbr_versions(self, min_version=None):
        if min_version is None:
            min_version = self.min_dbr_version
        response = self.api_call(None, "GET", "/api/2.0/clusters/spark-versions")
        items = json.loads(response.text)["versions"]
        out = {}
        for dbr in items:
            if any(tok in dbr["name"] for tok in self.invalid_dbr_list):
                continue
            if float(dbr["name"].split(" ")[0]) >= min_version:
                out[dbr["key"]] = dbr["name"]
        return collections.OrderedDict(sorted(out.items(), reverse=True))

    # ---------- Workflow-label defaults ----------

    def _init_workflow_defaults(self):
        self.default_workflow = self.workflows_dict["CLUSTER"]
        self.workflow_vals = list(self.workflows_dict.values())
        name_prefix = string.capwords(self.user_name).replace(" ", "-")
        wh_prefix = string.capwords(self.user_name).replace(" ", "_")
        self.default_job_name = f"{name_prefix}-TPCDI"
        self.default_wh = f"{wh_prefix}_TPCDI"

    # ---------- Cloud detection + lookups ----------

    def _init_cloud_defaults(self):
        self.uc_enabled = eval(
            string.capwords(self.spark.conf.get("spark.databricks.unityCatalog.enabled")))
        self.cloud_provider = self.spark.conf.get("spark.databricks.cloudProvider")
        self.node_types = self.get_node_types()
        self.dbrs = self.get_dbr_versions()
        self.default_dbr_version = list(self.dbrs.keys())[0]
        self.default_dbr = list(self.dbrs.values())[0]

    # ---------- Cloud-specific compute + catalog default ----------

    def _init_compute_and_catalog_defaults(self):
        if self.cloud_provider == "AWS":
            self.default_worker_type = "m7gd.2xlarge"
            self.default_driver_type = "m7gd.xlarge"
            self.cust_mgmt_type = "m7gd.16xlarge"
        elif self.cloud_provider == "GCP":
            self.default_worker_type = "n2-standard-8"
            self.default_driver_type = "n2-standard-4"
            self.cust_mgmt_type = "n2-standard-64"
            self.worker_cores_mult = self.worker_cores_mult * 1.5
        elif self.cloud_provider == "Azure":
            self.default_worker_type = "Standard_D8ads_v5"
            self.default_driver_type = "Standard_D4as_v5"
            self.cust_mgmt_type = "Standard_D64ads_v5"
        self.default_catalog = "tpcdi" if self.uc_enabled else "hive_metastore"
