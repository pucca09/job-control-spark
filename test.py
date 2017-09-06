MAX_P(10)
#Data_Clean
priority=0
task_names=[]
for month in months:
    priority+=1
    for province in provinces:
        for user_type in ['Local', 'Other', '177']:
            input_path = os.path.join(input_dir, 'Source', province, month, user_type)
            output_path = os.path.join(output_dir, 'DataClean', province, month, user_type)
            task_name = 'DataClean_%s_%s_%s' %(province, month, user_type)
            task_names.append(task_name)
            PUT_TASK(task_name, 'python --conf=%s --input=%s --output=%s' % (os.path.join(project_dir_manager.python_root(), 'pipeline/data_clean.py'), 'data_clean.conf', input_path, output_path), priority=priority)
PUT_TASK('data_clean_done', 'echo data_clean done.', priority=1, deps=','.join(task_names))    

#DeOscillation
priority=0
task_names=[]
for month in months:
    priority+=1
    for province in provinces:
        for user_type in ['Local', 'Other', '177']:
            input_path = os.path.join(input_dir, 'DataClean', province, month, user_type)
            output_path = os.path.join(output_dir, 'DeOscillation', province, month, user_type)
            task_name = 'DeOscillation_%s_%s_%s' %(province, month, user_type)
            task_names.append(task_name)
            PUT_TASK(task_name, 'python --conf=%s --input=%s --output=%s' % (os.path.join(project_dir_manager.python_root(), 'pipeline/de_oscillation.py'), 'de_oscillation.conf', input_path, output_path), priority=priority, deps='data_clean_done')
            #deps='DataClean_%s_%s_%s' %(province, month, user_type)
            #PUT_TASK(task_name, 'python --conf=%s --input=%s --output=%s' % (os.path.join(project_dir_manager.python_root(), 'pipeline/de_oscillation.py'), 'de_oscillation.conf', input_path, output_path), priority=priority, deps=deps)
PUT_TASK('de_oscillation_done', 'echo de_oscillation done.', priority=1, deps=','.join(task_names))


 
