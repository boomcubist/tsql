CREATE TABLE #filenames([filename] varchar(100));

BULK INSERT #filenames FROM 'C:\Claritas\FilesFound.txt'

DECLARE @FromDate datetime2 = '2018-01-28 00:00:00.0000';

with GriffeyeFiles as 
(
-- Converted images
SELECT CONCAT(CONVERT([varchar](512), Evidence.SHA1, 2),'.jpg') as FilePath
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 2 <> 0

UNION ALL

SELECT CONCAT(CONVERT([varchar](512), Evidence.SHA1, 2),'.jpg') as FilePath
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 4 <> 0

UNION ALL

SELECT CONCAT(CONVERT([varchar](512), Evidence.SHA1, 2),'.mp4') as FilePath
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 8 <> 0

UNION ALL
-- Video Collage
SELECT CONCAT(CONVERT([varchar](512), Evidence.SHA1, 2),'.jpg') as FilePath
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 16 <> 0

UNION ALL
-- FrameContainers
SELECT CONCAT(CONVERT([varchar](512), Evidence.SHA1, 2),'.fc') as FilePath
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 32 <> 0

UNION ALL
-- Videntifier Signatures
SELECT CONCAT(CONVERT([varchar](512), Evidence.SHA1, 2),'.desc72') as FilePath
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 64 <> 0

UNION ALL
-- FrameDifference
SELECT CONCAT(CONVERT([varchar](512), Evidence.SHA1, 2),'.framediff') as FilePath
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 128 <> 0

UNION ALL
-- Nudity
SELECT CONCAT(CONVERT([varchar](512), Evidence.SHA1, 2),'.nudity') as FilePath
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 256 <> 0

UNION ALL
-- AudioPeaks
SELECT CONCAT(CONVERT([varchar](512), Evidence.SHA1, 2),'.audiopeaks') as FilePath
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 512 <> 0

UNION ALL
-- F1Signature
SELECT CONCAT(CONVERT([varchar](512), Evidence.SHA1, 2),'.f1') as FilePath
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 1024 <> 0;

)
select * from GriffeyeFiles 
where FilePath not in (select filename from #filenames)

drop table #filenames