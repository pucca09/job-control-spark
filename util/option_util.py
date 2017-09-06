# -*- coding: UTF-8 -*-
import sys
import getopt

def help():
   print "some.py ['-f '|'--conf=']config_file ['-i '|'--input=']input_dir ['-o '|'--output=']output_dir ['-m '|'--month=']month ['-p '|'--province=']province ['-l '|'--logfile=']log_file ['-h'|'--help']"
    #check(arg_map)
   print "\t['-f '|'--conf=']conf_file\t\t指定配置文件"
   print "\t['-i '|'--input=']input_dir\t\t指定输入路径"
   print "\t['-o '|'--output=']output_dir\t\t指定输出路径"
   print "\t['-m '|'--month=']month\t\t指定月份 201512"
   print "\t['-p '|'--province=']province\t\t指定省份: HaiNan" 
   print "\t['-l '|'--logfile=']log_file\t\t指定日志文件"
   print "\t['-h '|'--help']\t\t查看命令帮助"
 
def usage():
    help()
def check(arg_map):
    for key ,value in arg_map.items():
        if not value and key != 'params':
            print >> sys.stderr, 'the parameters is error'
            usage()
            sys.exit(1)

def get_option(argv):
    arg_map = {'conf': None,
                'input': None,
                'output': None,
                'month': None,
                'province':None,
                'params': list(),
                'logfile': '/tmp/tmp.log', }
    try:
        opts, args = getopt.getopt(argv[1:], "f:i:o:m:p:l:L:h", ["conf=", 'input=', 'output=', 'month=', 'province=', 'logfile=', 'help'])
    except getopt.GetoptError, err:
        # TODO need to change from line 11 to line 48
        # print help information and exit:
        print str(err)
        usage()
        sys.exit(2)
    #print opts
    for name, value in opts:
        if name in ("-f", "--conf"):
           arg_map['conf']=value
        elif name in ("-i", "--input"):
           arg_map['input']=value
        elif name in ("-o", "--output"):
           arg_map['output']=value
        elif name in ("-m", "--month"):
           arg_map['month']=value
        elif name in ("-p", "--province"):
           arg_map['province']=value
        elif name in ("-l", "--logfile"):
           arg_map['logfile']=value
        elif name in ("-h", "--help"):
           help()
           sys.exit(1)
        else:
           print 'name=%s\tvalue=%s' %(name,value)
           assert False, "unhandled option"

    if args:
        arg_map['params']=args
    if not arg_map.has_key('conf'):
        assert False, 'the configure file cannot be null' 
    print '----------------------------parameters--------------------------------'
    print arg_map
    print '----------------------------------------------------------------------'
    return arg_map

if __name__=='__main__':
    print get_option(sys.argv)
