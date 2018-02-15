CREATE TABLE #filenames([filename] varchar(100));

BULK INSERT #filenames FROM 'C:\Temp\FilesFound.txt'

select * from files 
where filename not in (select filename from #filenames)

drop table #filenames