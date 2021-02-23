from Mysql_helper import MYSQL

a = {}
mysql = MYSQL('qdm765045126_db')
select_sql = 'SELECT web_sysparams.Val, web_sysparamssub.`Name` from web_sysparams INNER JOIN web_sysparamssub on ' \
             'web_sysparams.Id = web_sysparamssub.PId where web_sysparams.Tp = "Applications";'
results = mysql.show_all(select_sql)
# print(results)

for result in results:
    try:
        a[result[0]]
        # print(result[0])
    except Exception as es:
        # print(es)
        if len(result[1]) > 4:
            a[result[0]] = result[1].split('   ')
        else:
            a[result[0]] = (result[1]+' ').split('   ')
    else:
        if len(result[1]) > 4:
            a[result[0]].append(result[1])
        else:
            a[result[0]].append(result[1]+' ')
print(a)

