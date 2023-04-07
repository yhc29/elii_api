data_version = "20220120"
SERVER_NAME = "IBM"

mongo_url = "mongodb://dbusername:dbpassword@xx.xx.xx:port/?authSource=admin&authMechanism=SCRAM-SHA-256"

db_elii_name = "optum_covid19_elii_20220120"
db_data_name = "optum_covid_development"
db_supplement_name = "db_supplement"
data_folder = ""
import_folder = ""
output_folder = ""

table_mapping = {"cov_pt":"PATIENT", "cov_rx_immun":"IMMUNIZATION", "cov_micro":"MICROBIOLOGY","cov_vis":"VISIT", "cov_prov":"PROVIDER", "cov_carearea":"CARE AREA", "cov_rx_patrep":"PATIENT REPORTED MEDS", "cov_rx_presc":"PRESCRIPTIONS WRITTEN", "cov_ins":"INSURANCE", "cov_enc":"ENCOUNTER", "cov_rx_adm":"MED ADMINISTRATIONS", "cov_diag":"DIAGNOSIS", "cov_lab":"LABS", "cov_enc_prov":"ENCOUNTER PROVIDER", "cov_obs":"OBSERVATIONS", "cov_proc":"PROCEDURE"}

date_info_list = [ ["cov_pt",["DECEASED_INDICATOR"],["BIRTH_YR","GENDER","RACE","ETHNICITY","REGION","DIVISION"],[["DATE_OF_DEATH"],None]], ["cov_enc",["INTERACTION_TYPE"],[],[["INTERACTION_DATE"],None]],["cov_diag",["DIAGNOSIS_CD","DIAGNOSIS_STATUS"],[],[["DIAG_DATE"],None] ],["cov_vis",["VISIT_TYPE","DISCHARGE_DISPOSITION"],[],[["VISIT_START_DATE"],None] ],["cov_carearea",["CAREAREA"],[],[["CAREAREA_DATE"],None] ],[ "cov_obs",["OBS_TYPE","OBS_RESULT","OBS_UNIT"],[],[["OBS_DATE","RESULT_DATE"],None] ],["cov_proc",["PROC_CODE","PROC_DESC","PROC_CODE_TYPE","BETOS_CODE","BETOS_DESC"],[],[["PROC_DATE"],None] ],["cov_rx_presc",["DRUG_NAME","NDC","GENERIC_DESC","DRUG_CLASS","ROUTE","DOSE_FREQUENCY","DISCONTINUE_REASON"],[],[["RXDATE"],None],None],["cov_rx_adm",["DRUG_NAME","NDC","GENERIC_DESC","DRUG_CLASS","ROUTE","DOSE_FREQUENCY","DISCONTINUE_REASON"],[],[["ORDER_DATE","ADMIN_DATE"],None] ],["cov_micro",["SPECIMEN_SOURCE","ORGANISM"],[],[["COLLECT_DATE","RESULT_DATE"],None] ],["cov_rx_immun",["MAPPED_NAME","IMMUNIZATION_DESC","NDC","PT_REPORTED"],[],[["IMMUNIZATION_DATE"],None] ],["cov_rx_patrep",["DRUG_NAME","NDC","GENERIC_DESC","DRUG_CLASS","ROUTE","DOSE_FREQUENCY"],[],[["REPORTED_DATE"],None] ],["cov_lab",["TEST_CODE","TEST_NAME","TEST_RESULT","RESULT_UNIT"],[],[["COLLECTED_DATE",'RESULT_DATE'],None] ],["cov_ins",["INSURANCE_TYPE"],[],[["INSURANCE_DATE"],None]],["cov_prov",[],[],[[],None]],["cov_enc_prov",[],[],[[],None]] ]
