SELECT SUBSTRING(e.MimeType, LEN(LEFT(e.MimeType, CHARINDEX ('/', e.MimeType))) + 1, LEN(e.MimeType) - LEN(LEFT(e.MimeType, CHARINDEX ('/', e.MimeType))) - LEN(RIGHT(e.MimeType, LEN(e.MimeType) - CHARINDEX ('.', e.MimeType))) - 1)
SELECT SUBSTRING(e.MimeType, LEN(SUBSTRING(e.MimeType, 0, LEN(e.MimeType) - CHARINDEX ('/', e.MimeType))) + 1, LEN(e.MimeType) - LEN(SUBSTRING(e.MimeType, 0, LEN(e.MimeType) - CHARINDEX ('/', e.MimeType))) - LEN(SUBSTRING(e.MimeType, CHARINDEX ('.', e.MimeType), LEN(e.MimeType))));