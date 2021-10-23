chcp 65001

set JAVA_OPTS= -Dfile.encoding=UTF-8 -Dconsole.encoding=UTF-8

set PATH=C:\DevTools\lb;%PATH%

liquibase.bat --driver=org.postgresql.Driver --logLevel=info --changeLogFile=changelog.xml --url="jdbc:postgresql://10.20.0.97:5432/long_task" --username=long_task --password=1 --liquibase-schema-name=public update
if errorlevel 1 goto err

goto ok

:err
echo !ERROR!
pause
exit

:ok
pause

