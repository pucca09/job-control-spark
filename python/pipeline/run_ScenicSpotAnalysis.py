# -*- coding: UTF-8 -*-
import sys
from jobControl import runner
from util import project_dir_manager, conf_parser, assert_message, hdfs_util, option_util
import os


def run(args):
    conf_file = args['conf']
    conf = conf_parser.ConfParser(conf_file)
    conf.load('ScenicAnalysis')            # 加载去震荡默认的参数配置模块
    month = args['month']
    if not month:
        if conf.has('month'):
            month = conf.get('month')
        else:
            assert False, 'the month is not setted'
    
    province = args['province']
    if not province:
        if conf.has('province'):
            province = conf.get('province')
        else:
            assert False, 'the province is not setted'
    cluster = conf.load_to_dict('cluster')
    

    scenicAnalysis_input = list()
    scenicAnalysis_params = list()
    print "Check input (file src is listed as follows)"
    #input 1  
    OtherProvTrueUserInfo_input = os.path.join(conf.get('root_dir'), 'DataClean', province, month, '%sOther' %month, '%sTrueOther.csv' %month)
    print "OtherProvTrueUserInfo:"+OtherProvTrueUserInfo_input
    scenicAnalysis_input.append(OtherProvTrueUserInfo_input)
    #input 2
    OtherProvStopPoint_input = os.path.join(conf.get('root_dir'), 'Extraction', province, month, '%sOtherStop.csv' %month)
    print "OtherProvStopPoint:"+OtherProvStopPoint_input
    scenicAnalysis_input.append(OtherProvStopPoint_input)
    #input 3
    LocalTrueUserInfo_input = os.path.join(conf.get('root_dir'), 'DataClean', province, month, '%sLocal' %month, '%sTrueLocal.csv' %month)
    print "LocalTrueUserInfo:"+LocalTrueUserInfo_input
    scenicAnalysis_input.append(LocalTrueUserInfo_input)
    #input 4
    CDRData_input = os.path.join(conf.get('root_dir'), 'SplitData', province, month, '%sCC.csv' %month)
    print "CDRData:"+CDRData_input
    scenicAnalysis_input.append(CDRData_input)
    #input 5
    ScenicData_input = os.path.join(conf.get('root_dir'), 'BasicInfo', 'ScenicSpot', '%s.csv' %province)
    print "ScenicData:"+ScenicData_input
    scenicAnalysis_input.append(ScenicData_input)
    #input 6
    CellData_input = os.path.join(conf.get('root_dir'), 'BasicInfo', 'BaseStation', '%s.csv' %province)
    print "CellData:"+CellData_input
    scenicAnalysis_input.append(CellData_input)
    #判断输入是否存在
    for inputfiles in scenicAnalysis_input:
        if not hdfs_util.exist(inputfiles):
           print >> sys.stderr, 'the input is not existed!'
           sys.exit(-1) 
    #插入jar包的第一个参数
    scenicAnalysis_params.append(conf.get('root_dir'))
    
    #插入Jar包的第二，第三个参数
    scenicAnalysis_params.append(province)
    scenicAnalysis_params.append(month)
    print 'scenic spot analysis start!'
    cluster['input_path'] = '/user/tele/trip'
    cluster['output_path'] = '/user/tele/trip/BackEnd/ScenicSpotAnalysis/HaiNan/201512'
    cluster['params'] = scenicAnalysis_params
    cluster['main_class'] = conf.load_to_dict('ScenicAnalysis').get('main_class')
    cluster['driver'] = conf.load_to_dict('ScenicAnalysis').get('driver')
    ScenicAnalysis_task = runner.SparkJob(**cluster)
    ScenicAnalysis_task.run()
    print 'scenic spot analysis end!'

if __name__=='__main__':
    run(option_util.get_option(sys.argv))
