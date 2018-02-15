/****** Script for SelectTopNRows command from SSMS  ******/
SELECT [DATE]
      ,[TIME]
      ,[s-ip]
      ,[cs-method]
      ,[cs-uri-stem]
      ,[cs-uri-query]
      ,[s-port]
      ,[s-username]
      ,[c-ip]
      ,[cs(User-Agent)]
      ,[cs(Referer)]
      ,[sc-STATUS]
      ,[sc-substatus]
      ,[sc-win32-STATUS]
      ,[time-taken]
  FROM [Investigations].[caid].[bfd-app-app01-iis]
  --where [cs-uri-query] like '%allow-no-sandbox-job%' 
  --where [c-ip] = '51.231.225.49'
  --where left([cs(Referer)],5) != 'https'
  --and left(date,1) != '#'
  --and [cs(referer)] != '-'
  where [sc-status] not in ('200','304', ) and left(date,1) !='#'
  order	by date, time


  select*
FROM [Investigations].[caid].[bfd-app-app01-iis]
where [sc-status ] = '302' and [s-username] != 'claire.tapp'


/****** Script for SelectTopNRows command from SSMS  ******/
SELECT *
  FROM [Investigations].[caid].[bfd-app-app01-iis]
  where date = '2018-01-25'
  order by time


/****** Script for SelectTopNRows command from SSMS  ******/
SELECT *
  FROM [Investigations].[caid].[bfd-app-app01-iis]
--  where left([cs(Referer)],5) != 'https'
-- and left(date,1) != '#'
--  and [cs(referer)] != '-'
where [cs(referer)] = 'http://il4web.cumpol.net/'
  order	by date, time