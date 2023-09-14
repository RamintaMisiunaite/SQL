CREATE PROC DynamicSatLoad (@sat VARCHAR(128))
AS
BEGIN
	--GET SOURCE STAGE TABLE---
	DECLARE @source_table VARCHAR(128)

	SET @source_table = (
			SELECT Stage
			FROM MapSatToSource
			WHERE Sat = @sat
			)

	PRINT @source_table

	--- GET ALL SAT COLUMNS ---
	DECLARE @source_columns VARCHAR(max)

	SET @source_columns = (
			SELECT string_agg(COLUMN_NAME, ', ') AS col_list
			FROM information_schema.columns
			WHERE table_name = @sat
			)

	PRINT @source_columns

	-- GET COLS THAT ARE IN STAGE TABLE-- 
	DECLARE @stage_columns VARCHAR(max)

	SET @stage_columns = (
			SELECT string_agg(COLUMN_NAME, ', ') AS col_list
			FROM information_schema.columns
			WHERE table_name = @source_table
			)

	PRINT @stage_columns

	-- GET PRIMARY KEYS from stage (for hub key) --- 
	DECLARE @primary_keys VARCHAR(max)

	SET @primary_keys = (
			SELECT C.COLUMN_NAME
			FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T
			JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C
				ON C.CONSTRAINT_NAME = T.CONSTRAINT_NAME
			WHERE C.TABLE_NAME = @source_table
				AND T.CONSTRAINT_TYPE = 'PRIMARY KEY'
			)

	PRINT @primary_keys

	-- get hub key--
	DECLARE @HubKey VARCHAR(128)

	SELECT @HubKey = QUOTENAME(name)
	FROM sys.columns
	WHERE object_id = OBJECT_ID(@sat)
		AND sys.columns.name LIKE 'Hub%Key';

	----------------
	DECLARE @sql NVARCHAR(max)

	SET @sql = N'
		

			---copy from stage to temp---
			SELECT CONVERT(VARCHAR(128), HASHBYTES(''MD5'', ' + @primary_keys + '), 2) AS ' + @HubKey + ',
				   CONVERT(VARCHAR(128), HASHBYTES(''MD5'', ''stage.' + @source_table + '''), 2) AS [SatRecordSourceId],
				   CHECKSUM(' + @stage_columns + ') AS [SatRecordHash],
				   GETDATE() AS [SatLoadDate],
				   NULL AS [SatEndDate],
				   ' + @stage_columns + ' 
			INTO #temp
			FROM stage.' + @source_table + ' s
		
			print ''sssss'';
			--update sat pagal # kur nesutampa haskeys -- 
			UPDATE dbo.' + @sat + '
			SET ' + @sat + '.SatEndDate = GETDATE()
			FROM #temp tmp
			WHERE 
				dbo.' + @sat + '.' + @HubKey + ' = tmp.' + @HubKey + '
				and dbo.' + @sat + '.SatEndDate is null
				and dbo.' + @sat + '.SatRecordHash <> tmp.SatRecordHash
			
			print ''sssss'';

			

			INSERT INTO dbo.' + @sat + ' (' + @source_columns + ')
			SELECT DISTINCT
				  ' + @HubKey + ',
				  [SatRecordSourceId],
				  [SatRecordHash],
				  [SatLoadDate],
				  [SatEndDate],
				  ' + 
		@stage_columns + ' 
			FROM #temp tmp
			WHERE NOT EXISTS 
			(
				SELECT 1 
				FROM dbo.' + @sat + '
				where ' + @sat + '.' + @HubKey + ' = tmp.' + @HubKey + '
				and ' + @sat + '.SatEndDate is null
			)

			drop table #temp'

	EXEC sp_executesql @sql;
END

EXEC DynamicSatLoad 'SAT_EMPLOYEES'

SELECT *
FROM SAT_ACCOUNTS

TRUNCATE TABLE SAT_INTL_ACCOUNTS

DROP PROC DynamicSatLoad
