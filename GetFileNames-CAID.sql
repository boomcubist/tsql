DECLARE @FromDate datetime2 = '2017-11-25 00:00:00.0000'

-- Original Files
SELECT CONCAT(FilestorePath.[Path],'\', 
'Original\', 
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 1, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 3, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 5, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 7, 2), '\',
CONVERT([varchar](512), Evidence.SHA1, 2),
Evidence.ExtensionOriginal) as FilePathOriginal
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate;

-- Converted images
SELECT CONCAT(FilestorePath.[Path],'\', 
'ImageConverted\', 
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 1, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 3, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 5, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 7, 2), '\',
CONVERT([varchar](512), Evidence.SHA1, 2),
'.jpg') as FilePathImageConverted
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 2 <> 0;

-- Thumbnail400 images
SELECT CONCAT(FilestorePath.[Path],'\', 
'Thumbnail400\', 
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 1, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 3, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 5, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 7, 2), '\',
CONVERT([varchar](512), Evidence.SHA1, 2),
'.jpg') as FilePathThumbnail400
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 4 <> 0;

-- Video Converted
SELECT CONCAT(FilestorePath.[Path],'\', 
'VideoConverted\', 
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 1, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 3, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 5, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 7, 2), '\',
CONVERT([varchar](512), Evidence.SHA1, 2),
'.mp4') as FilePathVideoConverted
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 8 <> 0;

-- Video Collage
SELECT CONCAT(FilestorePath.[Path],'\', 
'VideoCollage\', 
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 1, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 3, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 5, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 7, 2), '\',
CONVERT([varchar](512), Evidence.SHA1, 2),
'.jpg') as FilePathVideoCollage
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 16 <> 0;

-- FrameContainers
SELECT CONCAT(FilestorePath.[Path],'\', 
'FrameContainer\', 
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 1, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 3, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 5, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 7, 2), '\',
CONVERT([varchar](512), Evidence.SHA1, 2),
'.fc') as FilePathFrameContainer
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 32 <> 0;

-- Videntifier Signatures
SELECT CONCAT(FilestorePath.[Path],'\', 
'VidentifierSignature\', 
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 1, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 3, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 5, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 7, 2), '\',
CONVERT([varchar](512), Evidence.SHA1, 2),
'.desc72') as FilePathVidentifier
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 64 <> 0;

-- FrameDifference
SELECT CONCAT(FilestorePath.[Path],'\', 
'FrameDifference\', 
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 1, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 3, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 5, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 7, 2), '\',
CONVERT([varchar](512), Evidence.SHA1, 2),
'.framediff') as FilePathFrameDiff
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 128 <> 0;

-- Nudity
SELECT CONCAT(FilestorePath.[Path],'\', 
'Nudity\', 
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 1, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 3, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 5, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 7, 2), '\',
CONVERT([varchar](512), Evidence.SHA1, 2),
'.nudity') as FilePathNudity
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 256 <> 0;

-- AudioPeaks
SELECT CONCAT(FilestorePath.[Path],'\', 
'AudioPeaks\', 
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 1, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 3, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 5, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 7, 2), '\',
CONVERT([varchar](512), Evidence.SHA1, 2),
'.audiopeaks') as FilePathAudioPeaks
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 512 <> 0;

-- F1Signature
SELECT CONCAT(FilestorePath.[Path],'\', 
'F1Signature\', 
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 1, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 3, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 5, 2), '\',
SUBSTRING(CONVERT([varchar](512), Evidence.SHA1, 2), 7, 2), '\',
CONVERT([varchar](512), Evidence.SHA1, 2),
'.f1') as FilePathF1
FROM FilestorePath, Evidence 
WHERE Evidence.FilestorePathThumbsId = FilestorePath.Id
AND Evidence.DateEnteredUtc > @FromDate
AND Evidence.AvailableFileVersions & 1024 <> 0;
