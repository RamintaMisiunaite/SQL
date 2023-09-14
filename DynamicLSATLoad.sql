CREATE PROC DynamicLSatLoad (@lsat VARCHAR(128))
AS
BEGIN
	-- GET SOURCE TABLE ---
	DECLARE @source_table VARCHAR(128)

	SET @source_table = ''
	SET @source_table = (
			SELECT TOP (1) StageSource
			FROM ColMappingLSATS
			WHERE LSATDest = @lsat
			)

	--- GET ALL LSAT COLUMNS ---
	DECLARE @lsat_columns VARCHAR(max)

	SET @lsat_columns = ''
	SET @lsat_columns = (
			SELECT string_agg(COLUMN_NAME, ', ') AS col_list
			FROM information_schema.columns
			WHERE table_name = @lsat
			)

	-- GET COLS THAT ARE IN STAGE TABLE-- 
	DECLARE @stage_columns VARCHAR(max)

	SET @stage_columns = ''
	SET @stage_columns = (
			SELECT string_agg(StageCol, ', ') AS col_list
			FROM ColMappingLSATS
			WHERE LSATDest = @lsat
				AND isPK = 1
			)

	-- GET PRIMARY KEYS from stage --- 
	DECLARE @primary_keys VARCHAR(max)

	SET @primary_keys = ''
	SET @primary_keys = (
			SELECT string_agg(StageCol, ',''|'', ') AS col_list
			FROM ColMappingLSATS
			WHERE LSATDest = @lsat
				AND isPK = 1
			)

	-- GET LINKKEY--
	DECLARE @LSATKey VARCHAR(128)

	SELECT @LSATKey = QUOTENAME(name)
	FROM sys.columns
	WHERE object_id = OBJECT_ID(@lsat)
		AND sys.columns.name LIKE 'Link%Key';

	----- HUB KEYS FOR HUB..KEY ----
	DECLARE @HubList VARCHAR(max)

	SET @HubList = (
			SELECT string_agg(StageCol, ', ') AS col_list
			FROM ColMappingLSATS
			WHERE LSATDest = @lsat
				AND LSATCol LIKE 'Hub%'
			)

	--- create HubXKey rows ---
	DECLARE @HubRows VARCHAR(max)

	SET @HubRows = ''

	DECLARE @HubKey VARCHAR(max)

	WHILE len(@HubList) > 0
	BEGIN
		IF CHARINDEX(',', @HubList) > 0
		BEGIN
			SET @HubKey = SUBSTRING(@HubList, 1, CHARINDEX(',', @HubList) - 1)
			SET @HubList = REPLACE(@HubList, @HubKey + ', ', '')
		END
		ELSE IF CHARINDEX(',', @HubList) = 0
		BEGIN
			SET @HubKey = @HubList
			SET @HubList = LEFT(@HubList, LEN(@HubList) - LEN(@HubList))
		END

		IF @HubKey LIKE '%date%'
			SET @HubRows = @HubRows + 'CONVERT(VARCHAR(128),  HASHBYTES(''MD5'', CONVERT(VARCHAR(20),' + @HubKey + ',112)), 2) AS Hub' + @HubKey + 'Key ,'
		ELSE
			SET @HubRows = @HubRows + 'CONVERT(VARCHAR(128), HASHBYTES(''MD5'', ' + @HubKey + '), 2) AS Hub' + @HubKey + 'Key ,'
	END

	--- LOAD LSAT --- 
	DECLARE @sql NVARCHAR(max)

	SET @sql = N'
		
			SELECT 
				   CONVERT(VARCHAR(128), HASHBYTES(''MD5'', CONCAT (' + @primary_keys + ')), 2) AS ' + @LSATKey + ',' + @HubRows + '
				   CONVERT(VARCHAR(128), HASHBYTES(''MD5'', ''stage.' + @source_table + '''), 2) AS [LinkSatteliteRecordSourceId],
				   GETDATE() AS [LinkSatteliteLoadDate],
				   ' + @stage_columns + ',
				   NULL AS [LinkSatteliteEndDate],
				   CHECKSUM(' + @stage_columns + ') AS LinkSatteliteRecordHash
			INTO #temp
			FROM stage.' + @source_table + ' 
		
			
			UPDATE ' + @lsat + '
			SET [LinkSatteliteEndDate] = GETDATE()
			FROM #temp
			WHERE [dbo].' + @lsat + '.' + @LSATKey + '  = #temp.' + @LSATKey + '
				AND [dbo].' + @lsat + '.LinkSatteliteEndDate IS NULL
				AND #temp.LinkSatteliteRecordHash <> [dbo].' + @lsat + '.LinkSatteliteRecordHash


				
		  INSERT INTO [dbo].' + @lsat + '  (' + @lsat_columns + ')
		SELECT DISTINCT *
		FROM #temp
		WHERE NOT EXISTS (
				SELECT 1
				FROM [dbo].' + @lsat + ' lsat
				WHERE lsat.' + @LSATKey + ' = #temp.' + @LSATKey + 
		'
					AND lsat.LinkSatteliteEndDate IS NULL
				)

		delete from  dbo.' + @lsat + ' 
	    where  dbo.' + @lsat + '.HubAccountKey is NULL 
			
			
			drop table #temp'

	EXEC sp_executesql @sql;
END

EXEC DynamicLSatLoad 'LSAT_SALES_PIPELINE'

DROP PROC DynamicLSatLoad

SELECT *
FROM LSAT_SALES_PIPELINE

TRUNCATE TABLE LSAT_SALES_PIPELINE
