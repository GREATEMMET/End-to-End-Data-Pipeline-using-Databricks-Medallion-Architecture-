BASE_PATH = '/Volumes/workspace/bronze_schema/source_system'

INGESTION_CONFIG_1 = [
    # CRM
    {
        "source": "crm",
        "source_path": f"{BASE_PATH}/source_crm/cust_info.csv",
        "table": "crm_cust_info"
    },
    {
        "source": "crm",
        "source_path": f"{BASE_PATH}/source_crm/prd_info.csv",
        "table": "crm_prd_info"
    },
    {
        "source": "crm",
        "source_path": f"{BASE_PATH}/source_crm/sales_details.csv",
        "table": "crm_sales_details"
    },

    # ERP
      {
        "source": "erp",
        "source_path": f"{BASE_PATH}/source_erp/CUST_AZ12.csv",
        "table": "erp_cust_az12"
    },
    {
        "source": "erp",
        "source_path": f"{BASE_PATH}/source_erp/LOC_A101.csv",
        "table": "erp_loc_a101"
    },
    {
        "source": "erp",
        "source_path": f"{BASE_PATH}/source_erp/PX_CAT_G1V2.csv",
        "table": "erp_px_cat_g1v2"
    },

]

def make_config(source, sub_dir, file_name, table):

    return {
        "source": source,
        "source_path": f"{BASE_PATH}{sub_dir}{file_name}",
        "table": table
    }

INGESTION_CONFIG_2 = [
    make_config(source="crm", sub_dir="/source_crm/", file_name="cust_info.csv", table="crm_cust_info"),
    make_config(source='crm', sub_dir="/source_crm/", file_name="prd_info.csv", table="crm_prd_info"),
    make_config(source='crm', sub_dir='/source_crm/', file_name="sales_details.csv", table="crm_sales_details"),
    make_config(source="erp", sub_dir="/source_erp/",file_name="CUST_AZ12.csv", table="erp_cust_az12"),
    make_config(source="erp", sub_dir="/source_erp/",file_name="LOC_A101.csv", table="erp_loc_a101"),
    make_config(source="erp", sub_dir="/source_erp/",file_name="PX_CAT_G1V2.csv", table="erp_px_cat_g1v2")
]



