import pymongo
import os
from pathlib import Path
import ast
import re
from datetime import datetime

def load_config_file(filePath):
  if(os.path.isfile(filePath)):
    with open(filePath, "r") as f:
      for line in f:
        if " = " in line:
          key = line.strip().split(" = ")[0]
          value = line.strip().split(" = ")[1]
          if key == "data_version":
            data_version = value
          elif key == "mongo_url":
            mongo_url = value
          elif key == "db_tlii_name":
            db_tlii_name = value
          elif key == "db_data_name":
            db_data_name = value
          elif key == "data_folder":
            data_folder = value
          elif key == "import_folder":
            import_folder = value
          elif key == "output_folder":
            output_folder = value
          elif key == "mapping_file":
            mapping_file = value
          elif key == "corpus_file":
            corpus_file = value
          elif key == "table_mapping":
            table_mapping = ast.literal_eval(value)
          elif key == "date_info_list":
            date_info_list = ast.literal_eval(value)
    if not os.path.exists(import_folder):
      os.makedirs(import_folder)
    if not os.path.exists(output_folder):
      os.makedirs(output_folder)
    return data_version,mongo_url,db_tlii_name,data_folder,import_folder,output_folder,table_mapping,date_info_list
  else:
    return None

def save_list(my_list,name,type="ptid",op_folder="./"):
  file = open(op_folder + name + ".txt", "w")
  file.write("number of elements: " + str(len(my_list)) + "\n")
  for x in my_list:
    file.write(x + "\n")

def load_list(file_path,type="ptid"):
  my_list = []
  row_count = -1
  with open(file_path,'r') as file:
    for line in file:
        row_count+=1
        line = line.rstrip()
        if row_count > 0 and len(line)>1:
          my_list.append(line)
  return my_list

def ptid_intersection(ptid_list1,ptid_list2,cohort1_name="cohort1",cohort2_name="cohort2"):
  # print(cohort1_name, len(ptid_list1), cohort2_name, len(ptid_list2))
  if not ptid_list1 or not ptid_list2:
    return None,None,None,None
  with_list = list(set(ptid_list1) & set(ptid_list2))
  without_list = list(set(ptid_list1) - set(ptid_list2))
  with_perc = round(100*len(with_list)/len(ptid_list1),4)
  without_perc = round(100*len(without_list)/len(ptid_list1),4)
  # print("with", with_perc,"%","without", without_perc,"%")
  return with_list,without_list,with_perc,without_perc

def record_id_to_date_str(record_id):
  date_str = str(record_id).split(".")[0]
  return date_str[4:6]+"/"+date_str[6:]+"/"+date_str[:4]

def record_id_to_date(record_id):
  date_str = str(record_id).split(".")[0]
  month = int(date_str[4:6])
  day = int(date_str[6:])
  year = int(date_str[:4])
  return datetime(year, month, day)

def datestr_to_date(datestr_to_date,delimiter="-",format = "mdy"):
  try:
    if format == "mdy":
      month = int(datestr_to_date.split(delimiter)[0])
      day = int(datestr_to_date.split(delimiter)[1])
      year = int(datestr_to_date.split(delimiter)[2])
    elif format == "ymd":
      month = int(datestr_to_date.split(delimiter)[1])
      day = int(datestr_to_date.split(delimiter)[2])
      year = int(datestr_to_date.split(delimiter)[0])
    return datetime(year, month, day)
  except:
    return None

def validate_ndc(code):
  split_code = code.split("-")
  if len(split_code) == 2:
    split_code.append("")
  if len(split_code) != 3:
    return ""
  if len(split_code[0]) == 4:
    split_code[0] = "0"+split_code[0]
  if len(split_code[1]) == 3:
    split_code[1] = "0"+split_code[1]
  if len(split_code[2]) == 1:
    split_code[2] = "0"+split_code[2]
  if len(split_code[0]) == 5 and len(split_code[1]) == 4 and len(split_code[2]) == 2:
    return split_code[0]+split_code[1]+split_code[2]
  elif len(split_code[0]) == 5 and len(split_code[1]) == 4 and len(split_code[2]) == 0:
    return re.compile(split_code[0]+split_code[1]+"..")
  else:
    return ""
  
def create_ndc_list(code_list):
  result = []
  for code in code_list:
    new_code = validate_ndc(code)
    if new_code:
      result.append(new_code)
  return result

def date_to_str(input_date):
  month = input_date.month
  day = input_date.day
  year = input_date.year

  month_str = str(month) if month >= 10 else "0" + str(month)
  day_str = str(day) if day >= 10 else "0" + str(day)
  year_str = str(year)
  
  date_str = month_str + "/" + day_str + "/" + year_str
  return date_str