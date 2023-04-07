import pymongo
from datetime import datetime,timedelta
import re
import csv
import os
from collections import OrderedDict
import itertools
from multiprocessing import Pool, cpu_count
import bisect 
from pprint import pprint

from Utils import utils
from Utils.timer import Timer


class QueryClient:
  def __init__(self,url,db_tlii_name,output_path=None):
    self.__mongo_client = pymongo.MongoClient(url)
    self.db_tlii = self.__mongo_client[db_tlii_name]
    self.change_output_path(output_path)

  def change_output_path(self,new_path):
    if new_path:
      if not os.path.exists(new_path):
        os.makedirs(new_path)
      self.output_path = new_path
    else:
      self.output_path = None

  def value_search(self,value,col_name={"$exists":True},concept={"$exists":True}):
    result = {}
    docs = self.db_tlii["corpus"].find({"col_name":col_name,"concept":concept,"value":value})
    for doc in docs:
      this_col_name = doc["col_name"]
      this_concept = doc["concept"]
      this_value = doc["value"]
      try:
        result[this_col_name]
      except:
        result[this_col_name] = {}
      try:
        result[this_col_name][this_concept].append(this_value)
      except:
        result[this_col_name][this_concept] = [this_value]
    return result

  def basic_query(self,query,query_ptid_list=None,check_ptid_in_ap = None):
    """
    query = {col_name1:{mongo stmt}, col_name2:{mongo stmt}, ...}
    """
    ptid_stmt = []
    if check_ptid_in_ap == None:
      max_list_len = 100000
      check_ptid_in_ap = False
      if query_ptid_list == []:
        return []
      elif query_ptid_list and len(query_ptid_list)>max_list_len:
        pass
      elif query_ptid_list :
        check_ptid_in_ap = True
        ptid_stmt = [{ "$match" : {"ptid_list":{"$in":query_ptid_list} } },]

    ptid_result = []
    for col_name,stmt in query.items():
      _stmt = {"pt_group":{"$gte":0}}
      _stmt.update(stmt)
      ap_stmt = [
        { "$match" : _stmt},
        { "$project": { "ptid_list":1, "pt_group":1, "_id":0 }},
        { "$unwind" : "$ptid_list" }] \
        + ptid_stmt +\
        [{ "$group": {
            "_id":"$pt_group",
            "ptid_list":{"$addToSet":"$ptid_list"}
            }
        }]
        
      # pprint(self.db_tlii.command('aggregate', col_name+"_tii", pipeline=ap_stmt, explain=True))
      docs = self.db_tlii[col_name+"_tii"].aggregate(ap_stmt,allowDiskUse=False)
      for doc in docs:
        ptid_result += doc["ptid_list"]
      
      docs.close()
    if query_ptid_list:
      ptid_result = list(set(ptid_result) & set(query_ptid_list))

    return ptid_result
  
  def basic_query_intersection(self,col_name_a,query_a,col_name_b,query_b):
    ap_stmt = [
        { "$match" : query_a},
        { "$project": { "ptid_list":1, "pt_group":1, "_id":0 }},
        { "$unwind" : "$ptid_list" },
        { "$group": {
            "_id":"$pt_group",
            "ptid_list":{"$addToSet":"$ptid_list"}
            }
        },
        { "$unionWith": { "coll": col_name_b+"_tii", 
                          "pipeline": [ 
                            { "$match" : query_b},
                            { "$project": { "ptid_list":1, "pt_group":1, "_id":0 }},
                            { "$unwind" : "$ptid_list" },
                            { "$group": {
                                "_id":"$pt_group",
                                "ptid_list":{"$addToSet":"$ptid_list"}
                                }
                            } 
                          ]
        } },
        { "$group": {
            "_id":"$_id",
            "ptid_list":{"$push":"$ptid_list"}
            }
        },
        {"$project": { "ptid_list":{"$setIntersection": [ { "$arrayElemAt": [ "$ptid_list", 0 ] }, { "$arrayElemAt": [ "$ptid_list", 1 ] }, ]}, "pt_group":"_id" }}
        ]
    docs = self.db_tlii[col_name_a+"_tii"].aggregate(ap_stmt,allowDiskUse=False)
    ptid_result = []
    for doc in docs:
      ptid_result += doc["ptid_list"]
    docs.close()

    return ptid_result

  @staticmethod
  def generate_absolute_temporal_constaints(after_time,before_time):
    temporal_constraints = {}
    if not after_time:
      after_time = datetime(2000,1,1)
    if not before_time:
      before_time = datetime(2030,1,1)
    temporal_constraints["date"] = {"$gte":after_time,"$lte":before_time}
    return temporal_constraints

  def absolute_temporal_query(self,query,query_period,query_ptid_list=None):
    query_period = [None,None] if not query_period else query_period
    if query_period == [None,None]:
      return self.basic_query(query,query_ptid_list=query_ptid_list)

    after_time = query_period[0]
    before_time = query_period[1]
    temporal_constraints = self.generate_absolute_temporal_constaints(after_time,before_time)
    
    ptid_result = []
    for col_name,match_stmt in query.items():
      _match_stmt = {"pt_group":{"$gte":0}}
      _match_stmt.update(match_stmt.copy())
      _match_stmt.update(temporal_constraints)
      ap_stmt_part2 = [
        { "$unwind" : "$ptid_list" },
        { "$group": {
          "_id":"$pt_group",
          "ptid_list":{"$addToSet":"$ptid_list"}
          }
        }
      ]
      ap_stmt = [ 
        { "$match":_match_stmt },
        { "$project": { "_id":0, "pt_group":1, "ptid_list":1} }
      ]+ap_stmt_part2
      docs = self.db_tlii[col_name+"_tii"].aggregate(ap_stmt,allowDiskUse=False)
      count = 0 
      for doc in docs:
        ptid_result+=doc["ptid_list"]

      docs.close()
    
    if query_ptid_list:
      ptid_result = list(set(ptid_result) & set(query_ptid_list))

    return ptid_result


  def relative_temporal_query(self,query_a,query_b,query_period=None,query_ptid_list=None,return_type_a="first",return_type_b="last"):
    """
    query = {col_name1:{mongo stmt}, col_name2:{mongo stmt}, ...}
    """
    ptid_result = []
    p_timer = Timer()
    ptid_list_a = self.absolute_temporal_query(query_a.copy(),query_period,query_ptid_list=query_ptid_list)
    # print(p_timer.click())
    ptid_list_ab = self.absolute_temporal_query(query_b.copy(),query_period,query_ptid_list=ptid_list_a)
    # print(p_timer.click())

    event_dict_a = self.event_query_pt_timeline(query_a.copy(), query_period=query_period, query_ptid_list=ptid_list_ab, return_type=return_type_a)
    # print(p_timer.click())
    event_dict_b = self.event_query_pt_timeline(query_b.copy(), query_period=query_period, query_ptid_list=ptid_list_ab, return_type=return_type_b)
    # print(p_timer.click())
    # print(len(list(event_dict_b.keys())))
    for ptid,date_b in event_dict_b.items():
      try:
        if event_dict_a[ptid]<=date_b:
          ptid_result.append(ptid)
      except:
        pass
    return ptid_result

  def event_query_tii(self, query, query_period=None, query_ptid_list=None, return_type="first"):
    event_dict = {}
    if return_type == "first":
      group_op = "$min"
    elif return_type == "last":
      group_op = "$max"
    else:
      print("return_type should be first or last")
      return event_dict
    if query_ptid_list:
      ptid_stmt = [{"$match":{"ptid_list":{"$in":query_ptid_list}}}]
    else:
      ptid_stmt = []
    for col_name,stmt in query.items():
      # _stmt = stmt.copy()
      # _stmt.update({"pt_group":49})
      ap_stmt = [
        { "$match" : stmt},
        { "$project": { "ptid_list":1, "date":1, "_id":0 }},
        { "$unwind" : "$ptid_list" }] + ptid_stmt + [{ "$group": {
            "_id":"$ptid_list",
            "date":{group_op:"$date"}
            }}]

      docs = self.db_tlii[col_name+"_tii"].aggregate(ap_stmt,allowDiskUse=True)
      for doc in docs:
        try:
          if return_type == "first":
            event_dict[doc["_id"]] = min(doc["date"],event_dict[doc["_id"]])
          elif return_type == "last":
            event_dict[doc["_id"]] = max(doc["date"],event_dict[doc["_id"]])
        except:
          event_dict[doc["_id"]] = [doc["date"]]
    return event_dict

  def event_query_ori(self, query, query_period=None, query_ptid_list=None, return_type="first"):
    event_dict = {}
    if return_type == "first":
      group_op = "$min"
    elif return_type == "last":
      group_op = "$max"
    else:
      print("return_type should be first or last")
      return event_dict
    if query_ptid_list:
      ptid_stmt = {"PTID":{"$in":query_ptid_list}}
    else:
      ptid_stmt = {}
    for col_name,stmt in query.items():
      _stmt = {"pt_group":{"$gte":0}}
      _stmt.update(ptid_stmt)
      _stmt.update(stmt)
      _stmt.update({"date":{"$gt":datetime(1990,1,1)}})
      # print(_stmt)
      ap_stmt = [
        { "$match" : _stmt},
        { "$project": { "PTID":1, "date":1, "_id":0 }},
        { "$group": {
            "_id":"$PTID",
            "date":{group_op:"$date"}
            }}]

      docs = self.db_tlii[col_name+"_records"].aggregate(ap_stmt,allowDiskUse=False)
      for doc in docs:
        try:
          if return_type == "first":
            event_dict[doc["_id"]] = min(doc["date"],event_dict[doc["_id"]])
          elif return_type == "last":
            event_dict[doc["_id"]] = max(doc["date"],event_dict[doc["_id"]])
        except:
          event_dict[doc["_id"]] = [doc["date"]]
    return event_dict

  def event_query_pt_timeline(self, query, query_period=None, query_ptid_list=None, return_type="first"):
    event_dict = {}
    if return_type == "first":
      group_op = "$min"
      array_index = 0
    elif return_type == "last":
      group_op = "$max"
      array_index = -1
    else:
      print("return_type should be first or last")
      return event_dict
    if query_ptid_list:
      ptid_stmt = {"PTID":{"$in":query_ptid_list}}
    else:
      ptid_stmt = {}
    for col_name,stmt in query.items():
      _stmt = {"pt_group":{"$gte":0}}
      _stmt.update(ptid_stmt)
      _stmt.update(stmt)
      # print(_stmt)
      ap_stmt = [
        { "$match" : _stmt},
        { "$project": { "PTID":1, "date":{ "$arrayElemAt": [ "$date_list", array_index ] }, "_id":0 }},
        { "$group": {
            "_id":"$PTID",
            "date":{group_op:"$date"}
            }}]

      docs = self.db_tlii[col_name+"_pt_timeline"].aggregate(ap_stmt,allowDiskUse=False,batchSize=10000)
      for doc in docs:
        try:
          if return_type == "first":
            event_dict[doc["_id"]] = min(doc["date"],event_dict[doc["_id"]])
          elif return_type == "last":
            event_dict[doc["_id"]] = max(doc["date"],event_dict[doc["_id"]])
        except:
          event_dict[doc["_id"]] = [doc["date"]]
    return event_dict

  def event_relation_query_pt_timeline(self, col_name_a, query_a, col_name_b, query_b, return_type_a="first", return_type_b="last", query_period=None, query_ptid_list=None):
    event_dict = {}
    if return_type_a == "first":
      group_op_a = "$min"
      array_index_a = 0
    elif return_type_a == "last":
      group_op_a = "$max"
      array_index_a = -1
    else:
      print("return_type should be first or last")
      return event_dict
    if return_type_b == "first":
      group_op_b = "$min"
      array_index_b = 0
    elif return_type_b == "last":
      group_op_b = "$max"
      array_index_b = -1
    else:
      print("return_type should be first or last")
      return event_dict

    if query_ptid_list:
      ptid_stmt = {"PTID":{"$in":query_ptid_list}}
    else:
      ptid_stmt = {}

    _stmt_a = {"pt_group":{"$gte":0}}
    _stmt_a.update(ptid_stmt)
    _stmt_a.update(query_a)
    ap_stmt_a = [
      { "$match" : _stmt_a},
      { "$project": { "PTID":1, "date":{ "$arrayElemAt": [ "$date_list", array_index_a ] }, "_id":0 }},
      { "$group": {
          "_id":"$PTID",
          "date":{group_op_a:"$date"},
          }},
      {"$project":{"date":1,"event":"a"}}
      ]
    _stmt_b = {"pt_group":{"$gte":0}}
    _stmt_b.update(ptid_stmt)
    _stmt_b.update(query_b)
    ap_stmt_b = [
      { "$match" : _stmt_b},
      { "$project": { "PTID":1, "date":{ "$arrayElemAt": [ "$date_list", array_index_b ] }, "_id":0 }},
      { "$group": {
          "_id":"$PTID",
          "date":{group_op_b:"$date"},
          }},
      {"$project":{"date":1,"event":"b"}}
      ]
    ap_stmt = ap_stmt_a +\
      [ { "$unionWith": { "coll": col_name_b+"_pt_timeline", "pipeline": ap_stmt_b}},
        { "$group": {
          "_id":"$_id",
          "date_a":{"$max":{"$cond":[
            {"$eq":["$event","a"]},
            "$date",
            "$$REMOVE"
            ]}},
          "date_b":{"$max":{"$cond":[
            {"$eq":["$event","b"]},
            "$date",
            "$$REMOVE"
            ]}},
          }},
        {"$match":{"$expr":{"$lte":["$date_a", "$date_b"]}} },
        {"$project": {"_id":1}}
      ]
    docs = self.db_tlii[col_name_a+"_pt_timeline"].aggregate(ap_stmt,allowDiskUse=False,batchSize=10000)
    ptid_result = []
    count = 0
    for doc in docs:
      # count+=1
      # if count<=10:
      #   print(doc)
      # else:
      #   break
      ptid_result.append(doc["_id"])  
    return ptid_result

  


  @staticmethod
  def read_value_set_file(value_set_file):
    value_list = []
    with open(value_set_file,errors='ignore') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = -1
        for row in csv_reader:
            line_count += 1
            if line_count == 0:
                header = row
            else:
                value_list.append(row[0])
    return value_list
  def get_covid_positive(self,value_set_path):
    query_period = [datetime(2020,2,1),datetime(2099,12,31)]
    query_dict,query_dict_icd,query_dict_icd_other,query_dict_pcr,query_dict_ab,query_dict_ag,query_dict_uct = {},{},{},{},{},{},{}
    # ICD10 code
    covid_query = {"cov_diag": {"DIAGNOSIS_CD":{'$in':["U071","840539006"]},"DIAGNOSIS_STATUS":"Diagnosis of" }}
    # covid_ptid_list_by_diag = self.absolute_temporal_query(covid_query,query_period)
    covid_ptid_list_by_diag = self.basic_query(covid_query)
    query_dict["cov_diag"] = {"DIAGNOSIS_CD":{'$in':["U071","840539006"]},"DIAGNOSIS_STATUS":"Diagnosis of"}
    query_dict_icd["cov_diag"] = {"DIAGNOSIS_CD":{'$in':["U071","840539006"]},"DIAGNOSIS_STATUS":"Diagnosis of"}
    print("covid_ptid_list_by_diag",len(covid_ptid_list_by_diag))

    covid_query = {"cov_diag": {"DIAGNOSIS_CD":{'$in':["U071","840539006"]},"DIAGNOSIS_STATUS":{"$in":["Not recorded","Other diagnosis status","Possible diagnosis of","History of"]} } }
    # covid_ptid_list_by_diag_other = self.absolute_temporal_query(covid_query,query_period)
    covid_ptid_list_by_diag_other = self.basic_query(covid_query)
    query_dict["cov_diag"] = {"DIAGNOSIS_CD":{'$in':["U071","840539006"]} }
    query_dict_icd_other["cov_diag"] = {"DIAGNOSIS_CD":{'$in':["U071","840539006"]},"DIAGNOSIS_STATUS":{"$in":["Not recorded","Other diagnosis status","Possible diagnosis of","History of"]} }
    print("covid_ptid_list_by_diag_other",len(covid_ptid_list_by_diag_other))

     # test_result_list = self.read_value_set_file(value_set_path+"positive_test_result.csv")
    test_result_file_path = value_set_path + "covid_test_result_type_ord_reviewed.csv"
    with open(test_result_file_path,encoding='utf-8-sig') as f:
      csv_reader = csv.DictReader(f)
      test_result_list = []
      for row in csv_reader:
        if row['type'] == "+":
          test_result_list.append(row['test_result'])
    test_result_list = [ re.compile('^' + re.escape(x) + '$', re.IGNORECASE) for x in test_result_list ]
    test_result_list = self.get_value_set("cov_lab","TEST_RESULT",{"$in":test_result_list})

    test_name_file_path = value_set_path + "covid_test_name_type_ord_reviewed.csv"
    with open(test_name_file_path,encoding='utf-8-sig') as f:
      csv_reader = csv.DictReader(f)
      test_name_pcr_list,test_name_ab_list,test_name_ag_list,test_name_uct_list = [],[],[],[]
      for row in csv_reader:
        if row['type'] == "pcr":
          test_name_pcr_list.append(row['test_name'])
        elif row['type'] == "ab":
          test_name_ab_list.append(row['test_name'])
        elif row['type'] == "ag":
          test_name_ag_list.append(row['test_name'])
        else:
          test_name_uct_list.append(row['test_name'])

    # pcr test
    col_name = "cov_lab"
    loinc_code_pcr_list = self.read_value_set_file(value_set_path+"Loinc_codes_pcr.csv")
    # test_name_pcr_list = self.read_value_set_file(value_set_path+"test_name_pcr.csv")
    test_name_pcr_list = [ re.compile('^' + re.escape(x) + '$', re.IGNORECASE) for x in test_name_pcr_list ]
    test_name_pcr_list = self.get_value_set("cov_lab","TEST_NAME",{"$in":test_name_pcr_list})

    stmt = { "$or": [{"TEST_CODE":{"$in":loinc_code_pcr_list}}, {"TEST_NAME":{"$in":test_name_pcr_list}} ], "TEST_RESULT":{"$in":test_result_list} }
    pcr_query = {"cov_lab": stmt }
    query_dict_pcr["cov_lab"] = stmt
    # covid_ptid_list_by_pcr = self.absolute_temporal_query(pcr_query,query_period)
    covid_ptid_list_by_pcr = self.basic_query(pcr_query)
    print("covid_ptid_list_by_pcr",len(covid_ptid_list_by_pcr))

    # antibody test
    loinc_code_ab_list = self.read_value_set_file(value_set_path+"Loinc_codes_antibody.csv")
    # test_name_ab_list = self.read_value_set_file(value_set_path+"test_name_antibody.csv")
    test_name_ab_list = [ re.compile('^' + re.escape(x) + '$', re.IGNORECASE) for x in test_name_ab_list ]
    test_name_ab_list = self.get_value_set("cov_lab","TEST_NAME",{"$in":test_name_ab_list})

    stmt = { "$or": [{"TEST_CODE":{"$in":loinc_code_ab_list}}, {"TEST_NAME":{"$in":test_name_ab_list}} ], "TEST_RESULT":{"$in":test_result_list} }
    ab_query = {"cov_lab": stmt }
    query_dict_ab["cov_lab"] = stmt
    # covid_ptid_list_by_ab = self.absolute_temporal_query(ab_query,query_period)
    covid_ptid_list_by_ab = self.basic_query(ab_query)
    print("covid_ptid_list_by_ab",len(covid_ptid_list_by_ab))

    # antigen test
    loinc_code_ag_list = self.read_value_set_file(value_set_path+"Loinc_codes_antigen.csv")
    # test_name_ag_list = self.read_value_set_file(value_set_path+"test_name_antigen.csv")
    test_name_ag_list = [ re.compile('^' + re.escape(x) + '$', re.IGNORECASE) for x in test_name_ag_list ]
    test_name_ag_list = self.get_value_set("cov_lab","TEST_NAME",{"$in":test_name_ag_list})

    stmt = { "$or": [{"TEST_CODE":{"$in":loinc_code_ag_list}}, {"TEST_NAME":{"$in":test_name_ag_list}} ], "TEST_RESULT":{"$in":test_result_list} }
    ag_query = {"cov_lab": stmt }
    query_dict_ag["cov_lab"] = stmt
    # covid_ptid_list_by_ag = self.absolute_temporal_query(ag_query,query_period)
    covid_ptid_list_by_ag = self.basic_query(ag_query)
    print("covid_ptid_list_by_ag",len(covid_ptid_list_by_ag))

    # uncertain and other test
    loinc_code_uct_list = self.read_value_set_file(value_set_path+"Loinc_codes_uncertain.csv")
    # test_name_uct_list = self.read_value_set_file(value_set_path+"test_name_uncertain.csv")
    test_name_uct_list = [ re.compile('^' + re.escape(x) + '$', re.IGNORECASE) for x in test_name_uct_list ]
    test_name_uct_list = self.get_value_set("cov_lab","TEST_NAME",{"$in":test_name_uct_list})

    stmt = { "$or": [{"TEST_CODE":{"$in":loinc_code_uct_list}}, {"TEST_NAME":{"$in":test_name_uct_list}} ], "TEST_RESULT":{"$in":test_result_list} }
    uct_query = {"cov_lab": stmt }
    query_dict_uct["cov_lab"] = stmt
    # covid_ptid_list_by_uct = self.absolute_temporal_query(query_dict_uct,query_period)
    covid_ptid_list_by_uct = self.basic_query(query_dict_uct)
    print("covid_ptid_list_by_uct",len(covid_ptid_list_by_uct))

    # all
    covid_code_list = loinc_code_pcr_list+loinc_code_ab_list+loinc_code_ag_list+loinc_code_uct_list
    covid_test_name_list = test_name_pcr_list+test_name_ab_list+test_name_ag_list+test_name_uct_list
    query_dict["cov_lab"] = { "$or": [{"TEST_CODE":{"$in":covid_code_list}}, {"TEST_NAME":{"$in":covid_test_name_list}} ], "TEST_RESULT":{"$in":test_result_list} }

    covid_ptid_list = list(set(covid_ptid_list_by_diag+covid_ptid_list_by_diag_other+covid_ptid_list_by_pcr+covid_ptid_list_by_ab+covid_ptid_list_by_ag+covid_ptid_list_by_uct))
    print("covid_ptid_list",len(covid_ptid_list))
    result = {
      "covid":[covid_ptid_list,query_dict],
      "covid_diag":[covid_ptid_list_by_diag,query_dict_icd],
      "covid_diag_other":[covid_ptid_list_by_diag_other,query_dict_icd_other],
      "covid_pcr":[covid_ptid_list_by_pcr,query_dict_pcr],
      "covid_antibody":[covid_ptid_list_by_ab,query_dict_ab],
      "covid_antigen":[covid_ptid_list_by_ag,query_dict_ag],
      "covid_uncertain":[covid_ptid_list_by_uct,query_dict_uct],
    }
    return result
  
  def get_value_set(self,col_name=None,concept=None,value=None,freq=False):
    if not value:
      value = {"$exists":True}
    stmt = {"value":value}
    if col_name:
      stmt["col_name"] = col_name
    if concept:
      stmt["concept"] = concept
    docs = self.db_tlii["corpus"].find(stmt,{"value": 1, 'num_of_records': 1, "_id": 0})
    if freq:
      result = [ (doc["value"],doc['num_of_records']) for doc in docs ]
    else:
      result = [ doc["value"] for doc in docs ]
    return result
  
  def term_search(self,keyword_list,col_list = None,exact_match=False):
    stmt = {}
    if col_list:
      stmt = {"col_name":{"$in":col_list}}
    if exact_match:
      keyword_list = [ re.compile("^"+re.escape(x)+"$", re.IGNORECASE) for x in keyword_list ]
    else:
      keyword_list = [ re.compile(re.escape(x), re.IGNORECASE) for x in keyword_list ]
    stmt["value"] = {"$in":keyword_list}
    
    docs = self.db_tlii["corpus"].find(stmt,{"col_name": 1,"concept": 1,"value": 1, 'num_of_records': 1, "_id": 0})
    result = [ doc for doc in docs]
    return result

  @staticmethod
  def date_sort_filter(date_list,query_period):
    result = []
    date_list = sorted(date_list)
    if not query_period:
      return date_list

    for date in date_list:
      if date>=query_period[0] and date<=query_period[1]:
        result.append(date)
      elif date>query_period[1]:
        break
    return result

  def get_date_list(self,ptid_list,query_dict,query_period_dict,return_type="all"):
    result = {}
    for col_name,stmt in query_dict.items():
      _stmt = {"pt_group":{"$gte":0},"PTID":{"$in":ptid_list} }
      _stmt.update(stmt)
      ap_stmt =[
        { "$match" : _stmt},
        { "$project" : { "PTID":1,"date_list":1, "_id":0 }},
        { "$unwind" : "$date_list" },
        { "$group":
          {"_id": "$PTID",
          "date_list": {"$addToSet":"$date_list"} }
        }
      ]
      docs = self.db_tlii[col_name+"_pt_timeline"].aggregate(ap_stmt,allowDiskUse=False)
      for doc in docs:
        ptid = doc["_id"]
        date_list = doc["date_list"]
        if query_period_dict:
          query_period = query_period_dict[ptid]
        else:
          query_period = None
        date_list = self.date_sort_filter(date_list,query_period)
        if date_list:
          if return_type == "first":
            date_list = [date_list[0]]
          elif return_type == "last":
            date_list = [date_list[-1]]
          try:
            result[ptid] += [ [col_name,x] for x in date_list ]
          except:
            result[ptid] = [ [col_name,x] for x in date_list ]

    for ptid in result.keys():
      result[ptid] = sorted(result[ptid], key=lambda x: x[1])

    if return_type=="first":
      for ptid in result.keys():
        result[ptid] = result[ptid][0]
    elif return_type=="last":
      for ptid in result.keys():
        result[ptid] = result[ptid][-1]
    return result


  def get_num_of_record(self,ptid_list,query_dict,query_period_dict,cohort_period=None):
    result = {}
    cohort_period = [None, None] if not cohort_period else cohort_period
    # cohort_after_date = int('%d%02d%02d' % (cohort_period[0].year, cohort_period[0].month, cohort_period[0].day)) if cohort_period[0] else 0
    # cohort_before_date = int('%d%02d%02d' % (cohort_period[1].year, cohort_period[1].month, cohort_period[1].day)) if cohort_period[1] else 30000000
    # print(cohort_after_date,cohort_before_date)
    print(cohort_period)
    for ptid in ptid_list:
      query_period = [None, None] if not query_period_dict[ptid] else query_period_dict[ptid]
      # after_date = int('%d%02d%02d' % (query_period[0].year, query_period[0].month, query_period[0].day)) if query_period[0] else 0
      # before_date = int('%d%02d%02d' % (query_period[1].year, query_period[1].month, query_period[1].day)) if query_period[1] else 30000000
      # query_period_dict[ptid] = [after_date,before_date]
      query_period_dict[ptid] = query_period
    for col_name,query in query_dict.items():
      _stmt = {"pt_group":{"$gte":0}}
      _stmt.update({"PTID":{"$in":ptid_list}})
      _stmt.update(query)
      ap_stmt =[
        { "$match" : _stmt},
        { "$project" : {"PTID":1,"date_list":1,"_id":0}},
        { "$unwind" : "$date_list" },
        { "$match" : {"date_list":{"$gte":cohort_period[0],"$lte":cohort_period[1]} } },
        # may reach the document limit 16MB
        # { "$group" :
        #   {"_id": "$PTID",
        #   "record_id_list": {"$push":"$record_id_list"} }
        # }
      ]
      docs = self.db_tlii[col_name+"_pt_timeline"].aggregate(ap_stmt,allowDiskUse=False)
      for doc in docs:
        ptid = doc["PTID"]
        if doc["date_list"]>=query_period_dict[ptid][0] and doc["date_list"]<=query_period_dict[ptid][1] :
          try:
            result[ptid] += 1
          except:
            result[ptid] = 1

    return result

  def get_num_of_record_v2(self,ptid_list,query_dict,query_period_dict,cohort_period=None):
    result = {}
    cohort_period = [None, None] if not cohort_period else cohort_period
    print(cohort_period)
    ptid_count = 0
    for ptid in ptid_list:
      ptid_count+=1
      print(ptid_count)
      query_period = [None, None] if not query_period_dict[ptid] else query_period_dict[ptid]
      after_date = query_period[0] if query_period[0] else datetime(1900,1,1)
      before_date = query_period[1] if query_period[1] else datetime(2100,1,1)
      for col_name,query in query_dict.items():
        _stmt = {"pt_group":{"$gte":0}}
        _stmt.update({"PTID":ptid})
        _stmt.update(query)
        ap_stmt =[
          { "$match" : _stmt},
          { "$project" : {"PTID":0,"date_list":1,"_id":0}},
          { "$unwind" : "$date_list" },
          { "$match" : {"date_list":{"$gte":after_date,"$lte":before_date} } },
          # may reach the document limit 16MB
          { "$group" :
            {"_id": None,
            "event_count": {"$sum":1} }
          }
        ]
        docs = self.db_tlii[col_name+"_pt_timeline"].aggregate(ap_stmt,allowDiskUse=False)
        for doc in docs:
          result[ptid] = doc["event_count"]

    return result

  def merge_pt_visits(self,ptid,query_period=None):
    # continuous visit = [record_id_list,start_date,end_date]
    result = []
    query_period = [None, None] if not query_period else query_period
    after_date = query_period[0] if query_period[0] else datetime(1900,1,1)
    before_date = query_period[1] if query_period[1] else datetime(2100,1,1)
    pt_group = int(ptid[-2:])
    vis_reocrds = self.db_tlii["cov_vis_records"].find({"pt_group":pt_group,"PTID":ptid,"date":{"$gte":after_date,"$lte":before_date}}).sort("date",1)
    vis_reocrds = list(vis_reocrds)
    if not vis_reocrds:
      return None
    else:
      first_record = vis_reocrds[0]
      start_date_str = first_record['VISIT_START_DATE']
      start_date = datetime.strptime(start_date_str, '%m-%d-%Y')
      end_date_str = first_record['VISIT_END_DATE']
      end_date = datetime.strptime(end_date_str, '%m-%d-%Y')
      result = [ [[ first_record['date'] ],start_date, end_date] ]
    for record in vis_reocrds[1:]:
      start_date_str = record['VISIT_START_DATE']
      start_date = datetime.strptime(start_date_str, '%m-%d-%Y')
      end_date_str = record['VISIT_END_DATE']
      end_date = datetime.strptime(end_date_str, '%m-%d-%Y')
      if result == [] or start_date>result[-1][2]:
        # new visit
        result.append([ [ record['date'] ], start_date, end_date ])
      else:
        # merge to last visit
        result[-1][0].append(record['date'])
        if end_date>result[-1][2]:
          result[-1][2] = end_date
    return result

  def get_record_list(self,ptid_list,query_dict,query_period_dict,return_type="all"):
    result = {}
    for ptid in ptid_list:
      after_date = query_period_dict[ptid][0]
      before_date = query_period_dict[ptid][1]
      for col_name,query in query_dict.items():
        _stmt = {"pt_group":int(ptid[-2:]),"PTID":ptid}
        _stmt.update(query)
        ap_stmt =[
          { "$match" : _stmt},
          { "$project" : { "date_list":1, "_id":0 }},
          { "$unwind" : "$date_list" },
          { "$match" : {"date_list":{"$gte":after_date,"$lte":before_date}}},
          { "$group":
            {"_id": None,
            # todo first date or last date
            "date": {"$min":"$date_list"} }
          }
        ]
        docs = self.db_tlii[col_name+"_pt_timeline"].aggregate(ap_stmt,allowDiskUse=False)
        for doc in docs:
          try:
            result[ptid].append(doc["date"])
          except:
            result[ptid] = [doc["date"]]

    for ptid in result.keys():
      result[ptid] = sorted(result[ptid], key=lambda x: x)

    if return_type=="first":
      for ptid in result.keys():
        result[ptid] = result[ptid][0]
    elif return_type=="last":
      for ptid in result.keys():
        result[ptid] = result[ptid][-1]
    return result
