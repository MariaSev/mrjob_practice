import os
import sys
import json
import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep

OUTPUT_FILE_NAME = 'tmp'
JOIN_FIELD='NameOfStation'
FIELDS_SET=set()
FIRST_TABLE=''
SECOND_TABLE=''

def prepare_input(tmp_file, table1, tabel2):
    prepared = open(tmp_file,'w', encoding='utf-8')
    for table_name in (table1, tabel2):
        table = open(table_name,'r', encoding='utf-8-sig')
        print(table_name)
        rows = json.load(table)
        global FIELDS_SET
        FIELDS_SET = FIELDS_SET.union(set(rows[0].keys()))
        table.close()
        for r in rows:
            r['source']=table_name
            if table_name=='tpu.json':
                string_with_name=r['TPUName']
                r['NameOfStation']=string_with_name[string_with_name.find('«')+1:string_with_name.find('»')]
        for row in rows:
            prepared.write(json.dumps(row,ensure_ascii=False,separators = (',',':'))+'\n')
    prepared.close()
    return 0

def get_mr_res(job, runner):
    r = []
    for key, value in job.parse_output(runner.cat_output()):
        r.append([key, value])
    return r

def make_text(pairs):
    text='['
    for i in pairs:
        text+=i[0]+','
    text=text[:-1]
    text+=']'
    return text

def write_file(name, txt):
    f = open(name,'w', encoding='utf-8')
    f.write(txt)
    f.close()
    return 0

def coalecse_left_join(row1, row2):
    global FIELDS_SET
    res={}
    res.update(row1)
    if len(row2)==0:
        for i in FIELDS_SET:
            if i not in res:
                res.update({i:None})
    else:
        for i in row2:
            if i not in res:
                res.update({i:row2[i]})
    return res

def coalecse_right_join(row1, row2):
    global FIELDS_SET
    res={}
    res.update(row2)
    if len(row1)==0:
        for i in FIELDS_SET:
            if i not in res:
                res.update({i:None})
    else:
        for i in row1:
            if i not in res:
                res.update({i:row1[i]})
    return res

    
class MERGE(MRJob):
    def steps(self):
        return[MRStep(mapper=self.join_mapper,
                      reducer=self.join_reducer),
               MRStep(reducer=self.unit_reducer)]
    

    def join_mapper(self, _, line):
        units=json.loads('['+line+']')[0]
        key=units.pop(JOIN_FIELD)

    def join_reducer(self, key, value):        
        rows_from_first_table=[]
        rows_from_second_table=[]
        for i in value:
            if i['source']==FIRST_TABLE:
                rows_from_first_table.append(i)
            elif i['source']==SECOND_TABLE:
                rows_from_second_table.append(i)
        left_join_res=[]
        right_join_res=[]
        if len(rows_from_first_table)==0:
            for r in rows_from_second_table:
                right_join_res.append(coalecse_right_join({},r))
        elif len(rows_from_second_table)==0:
            for r in rows_from_first_table:
                left_join_res.append(coalecse_left_join(r,{}))
        else:
            for r1 in rows_from_first_table:
                for r2 in rows_from_second_table:
                    left_join_res.append(coalecse_left_join(r1,r2))
                    right_join_res.append(coalecse_right_join(r1,r2))
        for i in left_join_res+right_join_res:
            i.update({JOIN_FIELD:key})
            i.pop('source')
            yield json.dumps(i, sort_keys=True, ensure_ascii=False), 1
        
    def unit_reducer(self, key, value):
        yield key, 0

if __name__ == '__main__':
    args = sys.argv[1:]
    FIRST_TABLE, SECOND_TABLE = args[0], args[1]  
    OUTPUT_FILE_NAME = args[2]
    FIELDS_SET=set()
    prepare_input(OUTPUT_FILE_NAME, FIRST_TABLE, SECOND_TABLE)
    mr_job = MERGE([OUTPUT_FILE_NAME])
    with mr_job.make_runner() as runner:
        runner.run()
        job_res = get_mr_res(mr_job, runner)   
    write_file(OUTPUT_FILE_NAME, make_text(job_res))
