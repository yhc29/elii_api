from flask import Flask
from flask import jsonify
from flask_restful import Resource, Api, reqparse, marshal_with, request
from webargs import fields, validate
from webargs.flaskparser import use_args, use_kwargs, parser, abort
import pandas as pd
import ast

from Query.query import QueryClient
from Utils.timer import Timer
from Utils import utils
import re

import config.config_ibm as config_file

class HttpCode(object):
  ok = 200
  un_auth_error = 401
  params_error = 400
  server_error = 500
def restful_result(code, message, data):
  return jsonify({"code": code, "message": message, "data": data or {}})


app = Flask(__name__)
api = Api(app)

query_client = QueryClient(config_file.mongo_url,config_file.db_elii_name,config_file.output_folder)


class BasicQuery(Resource):
    # http://127.0.0.1:5000/query/basic?col_name=cov_diag&concept=DIAGNOSIS_CD&value=U071
    query_args = {
      "col_name": fields.Str(required=True), 
      "concept": fields.Str(required=True),
      "value": fields.Str(required=True)}
    
    @use_args(query_args, location="query")
    def get(self,args):
        query_stmt = { args['col_name'] : { args['concept']  : args['value'] } }
        # query_stmt = {'cov_diag':{'DIAGNOSIS_CD':'U071','DIAGNOSIS_CD_TYPE':'icd10'git }}
        ptid_list = query_client.basic_query(query_stmt)
        result = {'pt_count':len(ptid_list),'ptid_list':ptid_list[:10]}
        return restful_result(HttpCode.ok,'success',result)

class AbsoluteTemporalQuery(Resource):
    # http://127.0.0.1:5000/query/absolute_temporal?col_name=cov_diag&concept=DIAGNOSIS_CD&value=U071&start_date=2021-01-01T00:00:00&end_date=2021-02-01T00:00:00
    query_args = {
      "col_name": fields.Str(required=True), 
      "concept": fields.Str(required=True),
      "value": fields.Str(required=True),
      "start_date": fields.DateTime(required=True),
      "end_date": fields.DateTime(required=True)}
    
    @use_args(query_args, location="query")
    def get(self,args):
        query_stmt = { args['col_name'] : { args['concept']  : args['value'] } }
        query_period = [args['start_date'], args['end_date']]
        ptid_list = query_client.absolute_temporal_query(query_stmt,query_period)
        result = {'pt_count':len(ptid_list),'ptid_list':ptid_list[:10]}
        return restful_result(HttpCode.ok,'success',result)

class RelativeTemporalQuery(Resource):
    # http://127.0.0.1:5000/query/relative_temporal?col_name1=cov_diag&concept1=DIAGNOSIS_CD&value1=U071&col_name2=cov_diag&concept2=DIAGNOSIS_CD&value2=G63
    query_args = {
      "col_name1": fields.Str(required=True), 
      "concept1": fields.Str(required=True),
      "value1": fields.Str(required=True),
      "col_name2": fields.Str(required=True), 
      "concept2": fields.Str(required=True),
      "value2": fields.Str(required=True)}
    
    @use_args(query_args, location="query")
    def get(self,args):
        query_stmt1 = { args['col_name1'] : { args['concept1']  : args['value1'] } }
        query_stmt2 = { args['col_name2'] : { args['concept2']  : args['value2'] } }
        ptid_list = query_client.relative_temporal_query(query_stmt1,query_stmt2)
        result = {'pt_count':len(ptid_list),'ptid_list':ptid_list[:10]}
        return restful_result(HttpCode.ok,'success',result)

class ValueSearch(Resource):
    # http://127.0.0.1:5000/query/value_search?value=pfizer
    query_args = {
      "value": fields.Str(required=True)}
    
    @use_args(query_args, location="query")
    def get(self,args):
        value = re.compile(re.escape(args['value']), re.IGNORECASE)
        result = query_client.value_search(value)
        return restful_result(HttpCode.ok,'success',result)

class PatientInfo(Resource):
    # http://127.0.0.1:5000/patient/info?ptid=PT208504594
    query_args = {
      "ptid": fields.Str(required=True)}
    
    @use_args(query_args, location="query")
    def get(self,args):
      doc = query_client.db_tlii["cov_pt_records"].find_one({"pt_group":int(args['ptid'][-2:]),'PTID':args['ptid']})
      result = {'PTID':doc["PTID"], 'BIRTH_YR':doc['BIRTH_YR'], 'GENDER':doc['GENDER'], 'RACE':doc['RACE']}
      return restful_result(HttpCode.ok,'success',result)
class PtTimeline(Resource):
    # http://127.0.0.1:5000/patient/timeline?ptid=PT208504594
    query_args = {
      "ptid": fields.Str(required=True)}
    
    @use_args(query_args, location="query")
    def get(self,args):
        timeline = query_client.get_pt_timeline(args['ptid'])
        result = {'ptid':args['ptid'],'timeline':timeline}
        return restful_result(HttpCode.ok,'success',result)

api.add_resource(BasicQuery, '/query/basic')
api.add_resource(AbsoluteTemporalQuery, '/query/absolute_temporal')
api.add_resource(RelativeTemporalQuery, '/query/relative_temporal')
api.add_resource(ValueSearch, '/query/value_search')
api.add_resource(PatientInfo, '/patient/info')
api.add_resource(PtTimeline, '/patient/timeline')


if __name__ == '__main__':
    app.run()  # run our Flask app