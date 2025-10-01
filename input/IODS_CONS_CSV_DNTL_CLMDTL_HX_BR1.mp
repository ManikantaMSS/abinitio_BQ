@
{18|Type|XXGrepository|Primary_Name|XXGgraph|_ab_object_schema|0|_ab_semantic_schema|7|eme_flatten_format|1|schema_version|34|root_id|0|ent_count|132|rel_count|236|}
{2010001002|XXGdirectory|0|1|1|0|{@{}@}}
{2010600005|XXGgraph|1|0|1|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|_ab_semantic_schema|7|1|1|Hnl|@{0|}}
{30001002|XXparameter|_ab_has_render_fixups|2|1|1|Hl||{0|}}
{30001002|XXparameter|PROC_DEPTH|1|3|9|RFK||{0|}}
{30001002|XXparameter|IODS_GCS_TEMP|AB_PARAMETER_ENV|3|10|FKd|The parameter will be used in output file to write to bucket|{0|}}
{30001002|XXparameter|IODS_GCS_TEMP:sandbox|$IODS_GCS_TMP_BASE/temp/mfs_$\{PROC_DEPTH\}way|3|9||@{0|}}
{30001002|XXparameter|IODS_GCS_TEMP:eme|$IODS_GCS_TMP_BASE/temp|3|9||@{0|}}
{30001002|XXparameter|IODS_GCS_MFS|$IODS_GCS_TMP_BASE/temp|3|9|RFK|The parameter will be used to create the single directory MFS system|{0|}}
{30001002|XXparameter|DEL_IND|1|3|9|RFK|Indicator to delete the previous runs, 1 by default delets all previous data, 0 saves previous cycle dates|{0|}}
{30001002|XXparameter|CREATE_SUB_DIRS|1|3|9|RFK||{0|}}
{30001002|XXparameter|IODS_GCS_LOOKUP|$IODS_GCS_TMP_BASE/lookup|3|9|FK||{0|}}
{30001002|XXparameter|IODS_GCS_LOOKUP_TEMP|AB_PARAMETER_ENV|3|10|FKd|The parameter will be used in output file to write to bucket|{0|}}
{30001002|XXparameter|IODS_GCS_LOOKUP_TEMP:sandbox|$IODS_GCS_TMP_BASE/lookup/mfs_1way|3|9||@{0|}}
{30001002|XXparameter|IODS_GCS_LOOKUP_TEMP:eme|$IODS_GCS_TMP_BASE/lookup|3|9||@{0|}}
{30001002|XXparameter|AI_JOBNAME|IODS_CONS_CSV_DNTL_CLMDTL_HX_BR1|3|9|REFK||{0|}}
{30001002|XXparameter|FILECTLNUM|comes-from-plan|3|9|REFK||{0|}}
{30001002|XXparameter|CSVDNTL_START_DATE|comes-from-plan|3|9|REFK||{0|}}
{30001002|XXparameter|CSVDNTL_END_DATE|comes-from-plan|3|9|REFK||{0|}}
{30001002|XXparameter|TRG_DML_1|$AI_DML/IODS_CONS_CSV_DNTL_CLM_DTL_HX_final_write.dml|3|9|REFK|Output DML file name for V353S5|{0|}}
{30001002|XXparameter|TRG_DML_2|$AI_DML/STGDNTLCLMDTL_inserts_2019082807120189_ds.dml|3|9|REFK|Output DML file name for V377S0|{0|}}
{30001002|XXparameter|SRC_DML_1|$AI_DML/IODS_CONS_CSV_DNTL_CLMDTL_HX_BR1_V351S3P1_read_gen.dml|3|9|REFK|Input DML file name for V351S3P1|{0|}}
{30001002|XXparameter|!ab_compatibility|4.2.3.5|3|9|P|@{0|}}
{30001002|XXparameter|IODS_PUB_BQ_DATASET_ENR|comes-from-plan|3|2|RFK$||{0|}}
{30001002|XXparameter|IODS_PUB_BQ_DATASET_CNS|comes-from-plan|3|2|RFK$||{0|}}
}}@0|@0|0|0|0|0|0|0|IODS_CONS_CSV_DNTL_CLMDTL_HX_BR1|Generic User|@1|100|-1|@6|@2049|{0|}0|0|{0|}{0|}{0|}{0|}1.0|3461000|1194000|7|}}
{2010703001|XXGgraphinfo|2|0|3|0|{@{}@1|4.2.3.0|{1|1|}Job 'IODS_CONS_CSV_DNTL_CLMDTL_HX_BR1' Execution Status Report

***** Job execution failed at Thursday, October 24, 2024 20:22:20 *****
(Application Output may contain additional information describing the failure)
Run performed by user n956429
||@@@@31633919|}}
{2010705002|XXGrunsettings|3|0|5|0|{@{}@DEV GCP|@@@1|60|0|1|1|0|0|0|0|0|1|0|0|1|2|1|1|0|0|0|@80|}}
{2010242002|XXGface|4|0|7|0|{@{}@Arial|0|100|0|}}
{2010240003|XXGlegend|5|0|9|0|{@{}@aetna/111423/CSVConsolidation/IODS_CONS_CSV_DNTL_CLMDTL_HX_BR1
Graph Generated: 2023-11-14 15:26:55 Version: 21743|25000|694000|0|0|0|0|3|}}
{2010242002|XXGface|6|0|11|0|{@{}@Arial|0|200|0|}}
{2010240003|XXGlegend|7|0|13|0|{@{}@Version          Project/PCR number       
 Date Modified         Resource Type     
         Resource ID                     
                        Description ------------ 
        ---------------------------------- 
      ----------------------         ------------------------- 
             --------------------        
                                     ------------------- 
1.0                    P08-7826h         
              05-Apr-2019           Cognizant 
                  N122987(VenkataRangaSai,Peesapati) 
              Initial Version|25000|794000|0|0|0|0|23|}}
{2010242002|XXGface|8|0|15|0|{@{}@Arial|0|200|0|}}
{2010210004|XXGflow|9|0|17|0|{@{}@0|.5|.5|{0|}145|17|}}
{2010210004|XXGflow|10|0|19|0|{@{}@0|.5|.5|{0|}124|17|}}
{2010210004|XXGflow|11|0|21|0|{@{}@0|.5|.5|{0|}147|17|}}
{2010210004|XXGflow|12|0|23|0|{@{}@0|.5|.5|{0|}138|17|}}
{2010210004|XXGflow|13|0|25|0|{@{}@0|.5|.5|{0|}137|17|}}
{2010210004|XXGflow|14|0|27|0|{@{}@0|.5|.5|{0|}148|20|}}
{2010210004|XXGflow|15|0|29|0|{@{}@0|.5|.5|{0|}133|17|}}
{2010210004|XXGflow|16|0|31|0|{@{}@0|.5|.5|{0|}140|17|}}
{2010210004|XXGflow|17|0|33|0|{@{}@0|.5|.5|{0|}141|17|}}
{2010210004|XXGflow|18|0|35|0|{@{}@0|.5|.5|{0|}144|17|}}
{2010501005|XXGpvertex|19|0|37|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Dedup_Sorted.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|key|\{AK_UCK_ID; AK_UCK_ID_PREFIX_CD; AK_UCK_ID_SEGMENT_NO; AK_SUBMT_SVC_LN_NO\}|3|2|$|@{0|}}
{30001002|XXparameter|keep|first|3|1|l|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|dup_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|out_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject_metadata||3|8|s=|@{0|}}
}}@0|Dedup Sorted|1757000|90000|0|0|0|0|146|DEDU V353S0 Rmv Dup keycols|Ab Initio Software|Built-in 2.0:|1|100|0||6||36865|1|{1|0|}}}
{2010203004|XXGoport|20|0|39|0|{@{}@0|0|0|0|out|0.0|@@@2196|0|}}
{2010203004|XXGoport|21|0|42|0|{@{}@0|0|0|0|dup|0.0|@@@2192|0|}}
{2010203004|XXGoport|22|0|44|0|{@{}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|23|0|46|0|{@{}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|24|0|48|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|25|0|50|0|{@{}@0|0|0|0|in|0.0|@@@1936|0|}}
{2010501005|XXGpvertex|26|0|53|0|{Separates one specified data record in each group of data records from the rest of the records in the group.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|dedup|3|1|Hl||{0|}}
{30001002|XXparameter|image__|/~ab_home/bin/unitool|3|2|H$||{0|}}
{30001002|XXparameter|key||19|2|RFO$|Key on which to remove duplicates|{0|}}
{30001002|XXparameter|select||20|2|FK$|Optional select expression|{0|}}
{30001002|XXparameter|keep|first|15|1|FKl|Output record for a group of duplicates|{3|first|last|unique-only|}}
{30001002|XXparameter|package||8|1|FKl|Package containing transforms and data types|{0|}}
{30001002|XXparameter|package.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|error_group||3|2|F$|Optional group name identifying a Handle Errors component to which errors can be directed.|{0|}}
{30001002|XXparameter|error_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|log_group||3|2|F$|Optional group name of a Handle Logs component to which log output can be directed.|{0|}}
{30001002|XXparameter|log_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|reject_threshold|Abort on first reject|15|1|Fl|When to abort if input records are rejected|{3|Abort on first reject|Never abort|Use limit/ramp|}}
{30001002|XXparameter|reject_threshold.display_name|reject-threshold|3|9|P|@{0|}}
{30001002|XXparameter|limit|0|1|2|F$|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|limit.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|ramp|0.0|2|2|F$|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|limit_keyword|value reject-threshold Never* 0 value reject-threshold Abort* 0 sameas limit default dollar_substitution 0|1|13|Kv|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|ramp_keyword|value reject-threshold Never* 99.0 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword.condition|version :2.12.999.m999|3|15|P?|@{0|}}
{30001002|XXparameter|ramp_keyword2|value reject-threshold Never* -1 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword2.condition|version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|keyword_map|limit_keyword limit ramp_keyword ramp ramp_keyword2 ramp|3|2|$||{0|}}
{30001002|XXparameter|keyword_map.display_name|keyword-map|3|9|P|@{0|}}
{30001002|XXparameter|check_sort|True|13|1|FKl|Abort on first record that is out of sorted order|{0|}}
{30001002|XXparameter|check_sort.display_name|check-sort|3|9|P|@{0|}}
{30001002|XXparameter|check_sort.keyword|check-sort|3|9|P|@{0|}}
{30001002|XXparameter|check_sort.condition|version 2.11.1:|3|15|P?|@{0|}}
{30001002|XXparameter|check_sort.update_clauses|update_value False|3|9|P|@{0|}}
{30001002|XXparameter|logging|False|13|1|Fl|Log internal events|{0|}}
{30001002|XXparameter|log|log_concat|3|13|Kv|Special log parameter|{0|}}
{30001002|XXparameter|log.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_input||15|1|Fl|Frequency of input records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_input.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_output||15|1|Fl|Frequency of output records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_output.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_reject||15|1|Fl|Frequency of reject records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_reject.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|NoHidePorts|out|3|2|H$||{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|reject=in; out=in; dup=in|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis||3|2|H$||{0|}}
{30001002|XXparameter|filter_aggregate_analysis|out=filter(select); dup=filter(select)|3|2|H$||{0|}}
{30001002|XXparameter|num_types|2|1|2|H$|Number of private data types in package|{0|}}
{30001002|XXparameter|type1|error_info_t error_info "record string('', charset='x-ab-internal') component; int port_index; string('', charset='x-ab-internal') parameter; string('', charset='x-ab-internal') message; record string('', charset='x-ab-internal') code; int parent_index; record string('', charset='x-ab-internal') name; string('', charset='x-ab-internal') value; end[int] attributes; end[int] details; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|type2|log_event_t log_event "record string('', charset='x-ab-internal') event_type; string('', charset='x-ab-internal') event_text; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|operation1|optional out::output_for_error(error_info, in)|3|2|H$|Function to create output in case of error.|{0|}}
{30001002|XXparameter|operation2|optional error::make_error(error_info, in)|3|2|H$|Function to create error record in case of error.|{0|}}
{30001002|XXparameter|operation3|optional log_event::log_error(error_info, in)|3|2|H$|Function to create log record in case of error.|{0|}}
{30001002|XXparameter|operation4|optional log_event::final_log_output()|3|2|H$|Function to create final log record.|{0|}}
{30001002|XXparameter|num_operations|4|1|2|H$|Number of operations in package|{0|}}
{30001002|XXparameter|num_callbacks|4|1|2|H$|Number of callbacks in package|{0|}}
{30001002|XXparameter|_use_rich_error_format|True|13|1|Hl|False means use legacy string newline format|{0|}}
{30001002|XXparameter|_use_rich_log_format|True|13|1|Hl|False means use legacy native charset format|{0|}}
{30001002|XXparameter|Layout|@9|9|RFs||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|dup_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|reject_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|error_metadata|$AB_HOME/include/error-info.dml|7|2|Ff$||{0|}}
{30001002|XXparameter|log_metadata|$AB_HOME/include/log-info.dml|7|2|Ff$||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out,dup,reject,error,log::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1669401747|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagation|metadata type: error = remote("$AB_HOME/include/error-info.dml")
metadata type: reject = in
metadata type: log = remote("$AB_HOME/include/log-info.dml")|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through|metadata type: out = in
metadata type: dup = in
metadata type: in = out
metadata type: in = dup|3|1|FHKl|@{0|}}
}}@0|Dedup Sorted|0|0|0|0|0|0|0|@||1|100|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|27|0|55|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2196|0|}}
{2010203004|XXGoport|28|0|58|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|dup|0.0|@@@2192|0|}}
{2010203004|XXGoport|29|0|61|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|30|0|64|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|31|0|67|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|32|0|70|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1936|0|}}
{2010503005|XXGfvertex|33|0|73|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Output_File.mdc|3|2|Pf$|@{0|}}
{30001002|XXparameter|write_metadata|$AI_DML/IODS_CONS_CSV_DNTL_CLM_DTL_HX_final_write.dml|3|2|f$|@{0|}}
{30001002|XXparameter|eme_dataset_mapping|$[[record map_component 1 datasets [vector [record variable _interp_("write", "pdl") map_dataset 1 mapping [record dataset_path _interp_("$IODS_GCS_TEMP/DNTLCLM_DTL_inserts_2019082807120189.tmp", "pdl") create_update 1 is_db 0 dml_info NULL]]]]]|3|9||@{0|}}
{30001002|XXparameter|Layout|$IODS_GCS_TEMP/DNTLCLM_DTL_inserts_2019082807120189.tmp|3|2|$|@{0|}}
}}@0|Output File|3344000|106000|0|0|0|0|134|V353S5 DS CONS CSV DENTAL CLMDTL HX|Ab Initio Software|@1|100|0||6||32769|-1|-1|}}
{2010202004|XXGiport|34|0|75|0|{@{}@0|0|0|0|write|0.0|@@@1780|0|}}
{2010503005|XXGfvertex|35|0|78|0|{Represents one file, many files, or a multifile as an output from your graph.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
{30001002|XXparameter|mode|0x0062|1|2|FH$|modes of access|{0|}}
{30001002|XXparameter|create_directories|False|13|1|FKl|If True, create any parent directories as necessary|{0|}}
{30001002|XXparameter|create_directories.display_name|create-directories|3|9|P|@{0|}}
{30001002|XXparameter|create_directories.keyword|create-directories|3|9|P|@{0|}}
{30001002|XXparameter|create_directories.condition|version 4.1.1.r9.0:|3|15|P?|@{0|}}
{30001002|XXparameter|translate_format|False|13|1|Fl|If true, translate the file format.|{0|}}
{30001002|XXparameter|translate_format.condition|version 4.0.0.r1.0:|3|15|P?|@{0|}}
{30001002|XXparameter|format|uninterpreted|15|1|FKl|'uninterpreted' (raw data), 'seqfile' (Hadoop sequence file), 'parquet' (Parquet format), 'orc' (ORC format), 'avro' (Avro format), 'texttable' (Hive text table), 'hadoop_compressed' (Compressed raw data).|{7|uninterpreted|seqfile|parquet|orc|avro|texttable|hadoop_compressed|}}
{30001002|XXparameter|format.condition|param_exact translate_format True|3|15|P?|@{0|}}
{30001002|XXparameter|hadoopy_condition|value format uninterpreted False value format * True default constant False|13|13|Hv|Hidden condition for deciding hadoop-codec visibility|{0|}}
{30001002|XXparameter|hadoopy_condition.condition|param_exact translate_format True|3|15|P?|@{0|}}
{30001002|XXparameter|hadoop_codec|Uncompressed|15|1|FKl|Compression for writing hadoop specific file format. orc=\{ZLIB,SNAPPY,LZO\}, parquet=\{GZIP,SNAPPY,LZO\}, seqfile=\{GZIP,SNAPPY,Deflate,BZIP2,LZ4,LZO\}, avro=\{SNAPPY,Deflate,BZIP2\}, texttable=\{GZIP,SNAPPY,Deflate,BZIP2,LZ4,LZOP\}, hadoop_compressed=\{GZIP,SNAPPY,Deflate,BZIP2,LZ4,LZO,LZOP\}|{9|Uncompressed|GZIP|SNAPPY|Deflate|BZIP2|ZLIB|LZ4|LZO|LZOP|}}
{30001002|XXparameter|hadoop_codec.display_name|hadoop-codec|3|9|P|@{0|}}
{30001002|XXparameter|hadoop_codec.keyword|hadoop-codec|3|9|P|@{0|}}
{30001002|XXparameter|hadoop_codec.condition|param_exact hadoopy_condition True param_exact translate_format True|3|15|P?|@{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|key||19|2|RF$|Key specifier For Lookup File|{0|}}
{30001002|XXparameter|key.condition|param direct_addressed False mode lookup|3|15|P?|@{0|}}
{30001002|XXparameter|m_catalog_add_option_names|block_compressed keep_on_disk direct_addressed only_last_key_instance orc_file|3|2|H$||{0|}}
{30001002|XXparameter|eme_index_location||3|2|F$|Place in the EME to create a dataset corresponding to this index.|{0|}}
{30001002|XXparameter|eme_index_location.condition|mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|index_url||28|2|F$|The URL of the index for this lookup file (optional)|{0|}}
{30001002|XXparameter|index_url.condition|param direct_addressed False mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|keep_on_disk|False|13|1|Fl|Keep on Disk|{0|}}
{30001002|XXparameter|keep_on_disk.condition|mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|block_compressed|False|13|1|Fl|Block Compressed|{0|}}
{30001002|XXparameter|block_compressed.condition|mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|either_or|value block_compressed True True sameas keep_on_disk default constant True|13|13|v|Either B-C or K-o-D|{0|}}
{30001002|XXparameter|direct_addressed|False|13|1|Fl|Set to true if the block compressed lookup is direct addressed|{0|}}
{30001002|XXparameter|direct_addressed.condition|mode lookup version 2.15.5.r34: param_exact either_or True|3|15|P?|@{0|}}
{30001002|XXparameter|only_last_key_instance|False|13|1|Fl|Match only the last instance of each key|{0|}}
{30001002|XXparameter|only_last_key_instance.condition|mode lookup version 3.0.2.r12.0:|3|15|P?|@{0|}}
{30001002|XXparameter|orc_file|False|13|1|Fl|Set to true if it is Hive ORC file|{0|}}
{30001002|XXparameter|orc_file.condition|mode lookup version 3.2.2.r15.0: param_exact direct_addressed True param_exact block_compressed False|3|15|P?|@{0|}}
{30001002|XXparameter|Layout||28|9|RF||{0|}}
{30001002|XXparameter|eme_dataset_mapping||40|9|F|Place in the EME to create the dataset(s) corresponding to this component.|{0|}}
{30001002|XXparameter|write_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|mpcmodtime|1669401743|1|1|Hl|The last modification time of this component's template|{0|}}
}}@0|Output File|0|0|0|0|0|0|0|@|@1|100|-1|@6|@1|-1|-1|}}
{2010202004|XXGiport|36|0|80|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|write|0.0|@@@1780|0|}}
{2010501005|XXGpvertex|37|0|83|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Reformat.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|transform0|$AI_XFR/table_adaptor.xfr|3|2|f$|@{0|}}
{30001002|XXparameter|out0_metadata|$AI_DML/IODS_CONS_CSV_DNTL_CLMDTL_HX_BR1_V351S3P1_read.dml|3|2|f$|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject0_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
}}@0|Reformat|726000|80000|0|0|0|0|112|RFMT V351S3P1 Adaptor CSV 5010 DNTL CLMDTL|Ab Initio Software|Built-in 1.0:|1|100|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|38|0|85|0|{@{}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|39|0|88|0|{@{}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|40|0|90|0|{@{}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|41|0|92|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|42|0|94|0|{@{}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|43|0|97|0|{Changes the record format of your data by dropping fields or by using DML expressions to add fields, combine fields, or modify the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|reformat-transform|3|1|Hl||{0|}}
{30001002|XXparameter|image__|~ab_home/bin/unitool|3|2|H$||{0|}}
{30001002|XXparameter|count|1|1|1|Fl|Number of reformat transforms|{0|}}
{30001002|XXparameter|count.is_index|true|3|9|P|@{0|}}
{30001002|XXparameter|transform0||8|1|FOl|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
{30001002|XXparameter|select||20|2|FK$|Filter expression before reformatting|{0|}}
{30001002|XXparameter|error_group||3|2|F$|Optional group name identifying a Handle Errors component to which errors can be directed.|{0|}}
{30001002|XXparameter|error_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|log_group||3|2|F$|Optional group name of a Handle Logs component to which log output can be directed.|{0|}}
{30001002|XXparameter|log_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|reject_threshold|Abort on first reject|15|1|Fl|When to abort if input records are rejected|{3|Abort on first reject|Never abort|Use limit/ramp|}}
{30001002|XXparameter|reject_threshold.display_name|reject-threshold|3|9|P|@{0|}}
{30001002|XXparameter|limit|0|1|2|F$|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|limit.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|ramp|0.0|2|2|F$|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|limit_keyword|value reject-threshold Never* 0 value reject-threshold Abort* 0 sameas limit default dollar_substitution 0|1|13|Kv|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|ramp_keyword|value reject-threshold Never* 99.0 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword.condition|version :2.12.999.m999|3|15|P?|@{0|}}
{30001002|XXparameter|ramp_keyword2|value reject-threshold Never* -1 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword2.condition|version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|keyword_map|limit_keyword limit ramp_keyword ramp ramp_keyword2 ramp|3|2|$||{0|}}
{30001002|XXparameter|keyword_map.display_name|keyword-map|3|9|P|@{0|}}
{30001002|XXparameter|output_index||8|1|FKl|Output index computation|{0|}}
{30001002|XXparameter|output_index.display_name|output-index|3|9|P|@{0|}}
{30001002|XXparameter|output_index.keyword|output-index|3|9|P|@{0|}}
{30001002|XXparameter|output_index_port|discriminator|3|2|FHK$|Output index port for debugger|{0|}}
{30001002|XXparameter|output_index_port.display_name|output-index_port|3|9|P|@{0|}}
{30001002|XXparameter|output_index_port.keyword|output-index_port|3|9|P|@{0|}}
{30001002|XXparameter|output_index_signature|int output_index_out::output_index(in)|3|2|H$|Function returning index of output port|{0|}}
{30001002|XXparameter|output_index_signature.display_name|output-index_signature|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes||8|1|FKl|Output index vector computation|{0|}}
{30001002|XXparameter|output_indexes.display_name|output-indexes|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes.keyword|output-indexes|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_port|discriminator|3|2|FHK$|Output index port for debugger|{0|}}
{30001002|XXparameter|output_indexes_port.display_name|output-indexes_port|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_port.keyword|output-indexes_port|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_signature|int[int] output_indexes_out::output_indexes(in)|3|2|H$|Function returning vector of indexes of output ports|{0|}}
{30001002|XXparameter|output_indexes_signature.display_name|output-indexes_signature|3|9|P|@{0|}}
{30001002|XXparameter|logging|False|13|1|Fl|Log internal events|{0|}}
{30001002|XXparameter|log|log_concat|3|13|Kv|Special log parameter|{0|}}
{30001002|XXparameter|log.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_input||15|1|Fl|Frequency of input records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_input.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_output||15|1|Fl|Frequency of output records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_output.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_reject||15|1|Fl|Frequency of reject records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_reject.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|num_operations|7|1|2|H$|Number of operations in package|{0|}}
{30001002|XXparameter|operation1|out::reformat(in)|3|2|H$|Reformat operation|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out*|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|legacy_xfr;reject*=in;out*=in|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis||3|2|H$||{0|}}
{30001002|XXparameter|filter_aggregate_analysis|out=filter(select)|3|2|H$||{0|}}
{30001002|XXparameter|num_types|3|1|2|H$|Number of private data types in package|{0|}}
{30001002|XXparameter|type1|error_info_t error_info "record string('', charset='x-ab-internal') component; int port_index; string('', charset='x-ab-internal') parameter; string('', charset='x-ab-internal') message; record string('', charset='x-ab-internal') code; int parent_index; record string('', charset='x-ab-internal') name; string('', charset='x-ab-internal') value; end[int] attributes; end[int] details; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|type2|log_event_t log_event "record string('', charset='x-ab-internal') event_type; string('', charset='x-ab-internal') event_text; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|type3|event_info_t event_info "record int event_type; end"|3|2|H$|Information about the kind of event that happened|{0|}}
{30001002|XXparameter|operation2|optional out::output_for_error(error_info, in)|3|2|H$|Function to create output in case of error.|{0|}}
{30001002|XXparameter|operation3|optional error::make_error(error_info, in)|3|2|H$|Function to create error record in case of error.|{0|}}
{30001002|XXparameter|operation4|optional log_event::log_error(error_info, in)|3|2|H$|Function to create log record in case of error.|{0|}}
{30001002|XXparameter|operation5|optional log_event::final_log_output()|3|2|H$|Function to create final log record.|{0|}}
{30001002|XXparameter|operation6|optional out::output_at_event(event_info)|3|2|H$|Function to allow the output of an additional record at event|{0|}}
{30001002|XXparameter|operation7|optional ::on_startup(long in_recovery)|3|2|H$|Function executed at graph startup|{0|}}
{30001002|XXparameter|num_callbacks|6|1|2|H$|Number of callbacks in package|{0|}}
{30001002|XXparameter|_use_rich_error_format|True|13|1|Hl|False means use legacy string newline format|{0|}}
{30001002|XXparameter|_use_rich_log_format|True|13|1|Hl|False means use legacy native charset format|{0|}}
{30001002|XXparameter|Layout|@9|9|RFs||{0|}}
{30001002|XXparameter|main_mp_port|count out|3|1|l||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out0_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|reject0_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|error0_metadata|$AB_HOME/include/error-info.dml|7|2|Ff$||{0|}}
{30001002|XXparameter|log_metadata|$AB_HOME/include/log-info.dml|7|2|Ff$||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out[*],reject[*],error[*],log::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1669401747|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagation|metadata type: error = remote("$AB_HOME/include/error-info.dml")
metadata type: reject = in
metadata type: log = remote("$AB_HOME/include/log-info.dml")|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|Reformat|0|0|0|0|0|0|0|@||1|100|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|44|0|99|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
{30001002|XXparameter|transform0||8|8|FO=|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
}}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|45|0|103|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|46|0|106|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|47|0|109|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|48|0|112|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010505005|XXGtvertex|49|0|115|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Database/Input_Table.mdc|3|2|Pf$|@{0|}}
{30001002|XXparameter|config_file|$BQ_PUB_DB/bigquery_impersonate.dbc|3|2|f$|@{0|}}
{30001002|XXparameter|table_spec|SELECT
  A.UCK_ID AS AK_UCK_ID,
  A.UCK_ID_PREFIX_CD AS AK_UCK_ID_PREFIX_CD,
  A.UCK_ID_SEGMENT_NO AS AK_UCK_ID_SEGMENT_NO,
  A.SUBMT_SVC_LN_NO AS AK_SUBMT_SVC_LN_NO,
  A.ADJT_REPRC_CLM_NO_TXT,
  A.APLNC_PLCMT_DT,
  A.ASSIGNED_LN_NO,
  A.CLAIM_FORMAT_CD,
  A.CLM_PRIC_METHODOLOGY_CD,
  A.CLM_PRIC_PROD_SVC_ID,
  A.CLM_PRIC_PROD_SVC_ID_QLFR_CD,
  A.CLM_TYP_CD,
  A.CONTRACT_AMT,
  A.CONTRACT_CD,
  A.CONTRACT_PCT,
  A.CONTRACT_TYP_CD,
  A.CONTRACT_VER_ID,
  A.DIAG_CD_POINTER_01_CD,
  A.DIAG_CD_POINTER_02_CD,
  A.DIAG_CD_POINTER_03_CD,
  A.DIAG_CD_POINTER_04_CD,
  A.FILE_INFO_01_TXT,
  A.FILE_INFO_02_TXT,
  A.FILE_INFO_03_TXT,
  A.FILE_INFO_04_TXT,
  A.FILE_INFO_05_TXT,
  A.FILE_INFO_06_TXT,
  A.FILE_INFO_07_TXT,
  A.FILE_INFO_08_TXT,
  A.FILE_INFO_09_TXT,
  A.FILE_INFO_10_TXT,
  A.LN_ITEM_CHG_AMT,
  A.LN_ITEM_CTL_NO_TXT,
  A.ORAL_CAVITY_DESIGNATION_01_CD,
  A.ORAL_CAVITY_DESIGNATION_02_CD,
  A.ORAL_CAVITY_DESIGNATION_03_CD,
  A.ORAL_CAVITY_DESIGNATION_04_CD,
  A.ORAL_CAVITY_DESIGNATION_05_CD,
  A.OTPY_PRM_01_ID,
  A.OTPY_PRM_02_ID,
  A.OTPY_PRM_03_ID,
  A.OTPY_PRM_04_ID,
  A.OTPY_PRM_05_ID,
  A.PLCY_COMPLIANCE_CD,
  A.POS_CD,
  A.PRED_OF_BNFT_01_ID,
  A.PRED_OF_BNFT_02_ID,
  A.PRED_OF_BNFT_03_ID,
  A.PRED_OF_BNFT_04_ID,
  A.PRED_OF_BNFT_05_ID,
  A.PRED_OTPY_PRM_01_ID,
  A.PRED_OTPY_PRM_02_ID,
  A.PRED_OTPY_PRM_03_ID,
  A.PRED_OTPY_PRM_04_ID,
  A.PRED_OTPY_PRM_05_ID,
  A.PROC_CD,
  A.PROC_CD_DESC,
  A.PROC_CD_MODIFIER_01_CD,
  A.PROC_CD_MODIFIER_02_CD,
  A.PROC_CD_MODIFIER_03_CD,
  A.PROC_CD_MODIFIER_04_CD,
  A.PROC_CNT,
  A.PROD_SVC_ID_QLFR_CD,
  A.PROSTHESIS_CROWN_OR_INLAY_CD,
  A.PRR_AUTH_01_ID,
  A.PRR_AUTH_02_ID,
  A.PRR_AUTH_03_ID,
  A.PRR_AUTH_04_ID,
  A.PRR_AUTH_05_ID,
  A.PRR_AUTH_OTPY_PRM_01_ID,
  A.PRR_AUTH_OTPY_PRM_02_ID,
  A.PRR_AUTH_OTPY_PRM_03_ID,
  A.PRR_AUTH_OTPY_PRM_04_ID,
  A.PRR_AUTH_OTPY_PRM_05_ID,
  A.PRR_AUTH_OTPY_PRMID_QLFR_01_CD,
  A.PRR_AUTH_OTPY_PRMID_QLFR_02_CD,
  A.PRR_AUTH_OTPY_PRMID_QLFR_03_CD,
  A.PRR_AUTH_OTPY_PRMID_QLFR_04_CD,
  A.PRR_AUTH_OTPY_PRMID_QLFR_05_CD,
  A.PRR_PLCMT_DT,
  A.PRR_PLCMT_DT_QLFR_CD,
  A.REF_NO_01_TXT,
  A.REF_NO_02_TXT,
  A.REF_NO_03_TXT,
  A.REF_NO_04_TXT,
  A.REF_NO_05_TXT,
  A.REF_OTPY_PRM_ID_QLFR_01_CD,
  A.REF_OTPY_PRM_ID_QLFR_02_CD,
  A.REF_OTPY_PRM_ID_QLFR_03_CD,
  A.REF_OTPY_PRM_ID_QLFR_04_CD,
  A.REF_OTPY_PRM_ID_QLFR_05_CD,
  A.REJECT_RSN_CD,
  A.REPLCMT_DT,
  A.REPRC_ALLOWED_AMT,
  A.REPRC_APPRVD_HCPCS_CD,
  A.REPRC_APPRVD_SVC_UNT_CNT,
  A.REPRC_CLM_NO_TXT,
  A.REPRC_ORG_ID,
  A.REPRC_PER_DIEM_FLAT_RT_AMT,
  A.REPRC_SAVING_AMT,
  A.SALES_TAX_PAID_AMT,
  A.STK_UCK_ID,
  A.SVC_DT,
  A.TERMS_DISCNT_PCT,
  A.TOOTH_01_CD,
  A.TOOTH_01_SURFACE_01_CD,
  A.TOOTH_01_SURFACE_02_CD,
  A.TOOTH_01_SURFACE_03_CD,
  A.TOOTH_01_SURFACE_04_CD,
  A.TOOTH_01_SURFACE_05_CD,
  A.TOOTH_01_SURFACE_06_CD,
  A.TOOTH_01_SURFACE_07_CD,
  A.TOOTH_01_SURFACE_08_CD,
  A.TOOTH_01_SURFACE_09_CD,
  A.TOOTH_01_SURFACE_10_CD,
  A.TOOTH_01_SURFACE_11_CD,
  A.TOOTH_01_SURFACE_12_CD,
  A.TOOTH_01_SURFACE_13_CD,
  A.TOOTH_01_SURFACE_14_CD,
  A.TOOTH_01_SURFACE_15_CD,
  A.TOOTH_01_SURFACE_16_CD,
  A.TOOTH_01_SURFACE_17_CD,
  A.TOOTH_01_SURFACE_18_CD,
  A.TOOTH_01_SURFACE_19_CD,
  A.TOOTH_01_SURFACE_20_CD,
  A.TOOTH_01_SURFACE_21_CD,
  A.TOOTH_01_SURFACE_22_CD,
  A.TOOTH_01_SURFACE_23_CD,
  A.TOOTH_01_SURFACE_24_CD,
  A.TOOTH_01_SURFACE_25_CD,
  A.TOOTH_01_SURFACE_26_CD,
  A.TOOTH_01_SURFACE_27_CD,
  A.TOOTH_01_SURFACE_28_CD,
  A.TOOTH_01_SURFACE_29_CD,
  A.TOOTH_01_SURFACE_30_CD,
  A.TOOTH_01_SURFACE_31_CD,
  A.TOOTH_01_SURFACE_32_CD,
  A.TOOTH_02_CD,
  A.TOOTH_02_SURFACE_01_CD,
  A.TOOTH_02_SURFACE_02_CD,
  A.TOOTH_02_SURFACE_03_CD,
  A.TOOTH_02_SURFACE_04_CD,
  A.TOOTH_02_SURFACE_05_CD,
  A.TOOTH_02_SURFACE_06_CD,
  A.TOOTH_02_SURFACE_07_CD,
  A.TOOTH_02_SURFACE_08_CD,
  A.TOOTH_02_SURFACE_09_CD,
  A.TOOTH_02_SURFACE_10_CD,
  A.TOOTH_02_SURFACE_11_CD,
  A.TOOTH_02_SURFACE_12_CD,
  A.TOOTH_02_SURFACE_13_CD,
  A.TOOTH_02_SURFACE_14_CD,
  A.TOOTH_02_SURFACE_15_CD,
  A.TOOTH_02_SURFACE_16_CD,
  A.TOOTH_02_SURFACE_17_CD,
  A.TOOTH_02_SURFACE_18_CD,
  A.TOOTH_02_SURFACE_19_CD,
  A.TOOTH_02_SURFACE_20_CD,
  A.TOOTH_02_SURFACE_21_CD,
  A.TOOTH_02_SURFACE_22_CD,
  A.TOOTH_02_SURFACE_23_CD,
  A.TOOTH_02_SURFACE_24_CD,
  A.TOOTH_02_SURFACE_25_CD,
  A.TOOTH_02_SURFACE_26_CD,
  A.TOOTH_02_SURFACE_27_CD,
  A.TOOTH_02_SURFACE_28_CD,
  A.TOOTH_02_SURFACE_29_CD,
  A.TOOTH_02_SURFACE_30_CD,
  A.TOOTH_02_SURFACE_31_CD,
  A.TOOTH_02_SURFACE_32_CD,
  A.TOOTH_03_CD,
  A.TOOTH_03_SURFACE_01_CD,
  A.TOOTH_03_SURFACE_02_CD,
  A.TOOTH_03_SURFACE_03_CD,
  A.TOOTH_03_SURFACE_04_CD,
  A.TOOTH_03_SURFACE_05_CD,
  A.TOOTH_03_SURFACE_06_CD,
  A.TOOTH_03_SURFACE_07_CD,
  A.TOOTH_03_SURFACE_08_CD,
  A.TOOTH_03_SURFACE_09_CD,
  A.TOOTH_03_SURFACE_10_CD,
  A.TOOTH_03_SURFACE_11_CD,
  A.TOOTH_03_SURFACE_12_CD,
  A.TOOTH_03_SURFACE_13_CD,
  A.TOOTH_03_SURFACE_14_CD,
  A.TOOTH_03_SURFACE_15_CD,
  A.TOOTH_03_SURFACE_16_CD,
  A.TOOTH_03_SURFACE_17_CD,
  A.TOOTH_03_SURFACE_18_CD,
  A.TOOTH_03_SURFACE_19_CD,
  A.TOOTH_03_SURFACE_20_CD,
  A.TOOTH_03_SURFACE_21_CD,
  A.TOOTH_03_SURFACE_22_CD,
  A.TOOTH_03_SURFACE_23_CD,
  A.TOOTH_03_SURFACE_24_CD,
  A.TOOTH_03_SURFACE_25_CD,
  A.TOOTH_03_SURFACE_26_CD,
  A.TOOTH_03_SURFACE_27_CD,
  A.TOOTH_03_SURFACE_28_CD,
  A.TOOTH_03_SURFACE_29_CD,
  A.TOOTH_03_SURFACE_30_CD,
  A.TOOTH_03_SURFACE_31_CD,
  A.TOOTH_03_SURFACE_32_CD,
  A.TOOTH_04_CD,
  A.TOOTH_04_SURFACE_01_CD,
  A.TOOTH_04_SURFACE_02_CD,
  A.TOOTH_04_SURFACE_03_CD,
  A.TOOTH_04_SURFACE_04_CD,
  A.TOOTH_04_SURFACE_05_CD,
  A.TOOTH_04_SURFACE_06_CD,
  A.TOOTH_04_SURFACE_07_CD,
  A.TOOTH_04_SURFACE_08_CD,
  A.TOOTH_04_SURFACE_09_CD,
  A.TOOTH_04_SURFACE_10_CD,
  A.TOOTH_04_SURFACE_11_CD,
  A.TOOTH_04_SURFACE_12_CD,
  A.TOOTH_04_SURFACE_13_CD,
  A.TOOTH_04_SURFACE_14_CD,
  A.TOOTH_04_SURFACE_15_CD,
  A.TOOTH_04_SURFACE_16_CD,
  A.TOOTH_04_SURFACE_17_CD,
  A.TOOTH_04_SURFACE_18_CD,
  A.TOOTH_04_SURFACE_19_CD,
  A.TOOTH_04_SURFACE_20_CD,
  A.TOOTH_04_SURFACE_21_CD,
  A.TOOTH_04_SURFACE_22_CD,
  A.TOOTH_04_SURFACE_23_CD,
  A.TOOTH_04_SURFACE_24_CD,
  A.TOOTH_04_SURFACE_25_CD,
  A.TOOTH_04_SURFACE_26_CD,
  A.TOOTH_04_SURFACE_27_CD,
  A.TOOTH_04_SURFACE_28_CD,
  A.TOOTH_04_SURFACE_29_CD,
  A.TOOTH_04_SURFACE_30_CD,
  A.TOOTH_04_SURFACE_31_CD,
  A.TOOTH_04_SURFACE_32_CD,
  A.TOOTH_05_CD,
  A.TOOTH_05_SURFACE_01_CD,
  A.TOOTH_05_SURFACE_02_CD,
  A.TOOTH_05_SURFACE_03_CD,
  A.TOOTH_05_SURFACE_04_CD,
  A.TOOTH_05_SURFACE_05_CD,
  A.TOOTH_05_SURFACE_06_CD,
  A.TOOTH_05_SURFACE_07_CD,
  A.TOOTH_05_SURFACE_08_CD,
  A.TOOTH_05_SURFACE_09_CD,
  A.TOOTH_05_SURFACE_10_CD,
  A.TOOTH_05_SURFACE_11_CD,
  A.TOOTH_05_SURFACE_12_CD,
  A.TOOTH_05_SURFACE_13_CD,
  A.TOOTH_05_SURFACE_14_CD,
  A.TOOTH_05_SURFACE_15_CD,
  A.TOOTH_05_SURFACE_16_CD,
  A.TOOTH_05_SURFACE_17_CD,
  A.TOOTH_05_SURFACE_18_CD,
  A.TOOTH_05_SURFACE_19_CD,
  A.TOOTH_05_SURFACE_20_CD,
  A.TOOTH_05_SURFACE_21_CD,
  A.TOOTH_05_SURFACE_22_CD,
  A.TOOTH_05_SURFACE_23_CD,
  A.TOOTH_05_SURFACE_24_CD,
  A.TOOTH_05_SURFACE_25_CD,
  A.TOOTH_05_SURFACE_26_CD,
  A.TOOTH_05_SURFACE_27_CD,
  A.TOOTH_05_SURFACE_28_CD,
  A.TOOTH_05_SURFACE_29_CD,
  A.TOOTH_05_SURFACE_30_CD,
  A.TOOTH_05_SURFACE_31_CD,
  A.TOOTH_05_SURFACE_32_CD,
  A.TOOTH_06_CD,
  A.TOOTH_07_CD,
  A.TOOTH_08_CD,
  A.TOOTH_09_CD,
  A.TOOTH_10_CD,
  A.TOOTH_11_CD,
  A.TOOTH_12_CD,
  A.TOOTH_13_CD,
  A.TOOTH_14_CD,
  A.TOOTH_15_CD,
  A.TOOTH_16_CD,
  A.TOOTH_17_CD,
  A.TOOTH_18_CD,
  A.TOOTH_19_CD,
  A.TOOTH_20_CD,
  A.TOOTH_21_CD,
  A.TOOTH_22_CD,
  A.TOOTH_23_CD,
  A.TOOTH_24_CD,
  A.TOOTH_25_CD,
  A.TOOTH_26_CD,
  A.TOOTH_27_CD,
  A.TOOTH_28_CD,
  A.TOOTH_29_CD,
  A.TOOTH_30_CD,
  A.TOOTH_31_CD,
  A.TOOTH_32_CD,
  A.TRTMT_END_DT,
  A.TRTMT_START_DT,
  A.XCPTN_CD,
  COALESCE(B.ASRG_ENTY_TYP_QLFR_CD,'') AS ASRG_ENTY_TYP_QLFR_CD,
  COALESCE(B.ASRG_FST_NM,'') AS ASRG_FST_NM,
  COALESCE(B.ASRG_ID,'') AS ASRG_ID,
  COALESCE(B.ASRG_ID_QLFR_CD,'') AS ASRG_ID_QLFR_CD,
  COALESCE(B.ASRG_LST_NM,'') AS ASRG_LST_NM,
  COALESCE(B.ASRG_MID_NM,'') AS ASRG_MID_NM,
  COALESCE(B.ASRG_NM_SFX_TXT,'') AS ASRG_NM_SFX_TXT,
  COALESCE(B.ASRG_OTPY_PRM_01_ID,'') AS ASRG_OTPY_PRM_01_ID,
  COALESCE(B.ASRG_OTPY_PRM_02_ID,'') AS ASRG_OTPY_PRM_02_ID,
  COALESCE(B.ASRG_OTPY_PRM_03_ID,'') AS ASRG_OTPY_PRM_03_ID,
  COALESCE(B.ASRG_OTPY_PRM_04_ID,'') AS ASRG_OTPY_PRM_04_ID,
  COALESCE(B.ASRG_OTPY_PRM_05_ID,'') AS ASRG_OTPY_PRM_05_ID,
  COALESCE(B.ASRG_OTPY_PRM_06_ID,'') AS ASRG_OTPY_PRM_06_ID,
  COALESCE(B.ASRG_OTPY_PRM_07_ID,'') AS ASRG_OTPY_PRM_07_ID,
  COALESCE(B.ASRG_OTPY_PRM_08_ID,'') AS ASRG_OTPY_PRM_08_ID,
  COALESCE(B.ASRG_OTPY_PRM_09_ID,'') AS ASRG_OTPY_PRM_09_ID,
  COALESCE(B.ASRG_OTPY_PRM_10_ID,'') AS ASRG_OTPY_PRM_10_ID,
  COALESCE(B.ASRG_OTPY_PRM_11_ID,'') AS ASRG_OTPY_PRM_11_ID,
  COALESCE(B.ASRG_OTPY_PRM_12_ID,'') AS ASRG_OTPY_PRM_12_ID,
  COALESCE(B.ASRG_OTPY_PRM_13_ID,'') AS ASRG_OTPY_PRM_13_ID,
  COALESCE(B.ASRG_OTPY_PRM_14_ID,'') AS ASRG_OTPY_PRM_14_ID,
  COALESCE(B.ASRG_OTPY_PRM_15_ID,'') AS ASRG_OTPY_PRM_15_ID,
  COALESCE(B.ASRG_OTPY_PRM_16_ID,'') AS ASRG_OTPY_PRM_16_ID,
  COALESCE(B.ASRG_OTPY_PRM_17_ID,'') AS ASRG_OTPY_PRM_17_ID,
  COALESCE(B.ASRG_OTPY_PRM_18_ID,'') AS ASRG_OTPY_PRM_18_ID,
  COALESCE(B.ASRG_OTPY_PRM_19_ID,'') AS ASRG_OTPY_PRM_19_ID,
  COALESCE(B.ASRG_OTPY_PRM_20_ID,'') AS ASRG_OTPY_PRM_20_ID,
  COALESCE(B.ASRG_SCDID_QLFR_01_CD,'') AS ASRG_SCDID_QLFR_01_CD,
  COALESCE(B.ASRG_SCDID_QLFR_02_CD,'') AS ASRG_SCDID_QLFR_02_CD,
  COALESCE(B.ASRG_SCDID_QLFR_03_CD,'') AS ASRG_SCDID_QLFR_03_CD,
  COALESCE(B.ASRG_SCDID_QLFR_04_CD,'') AS ASRG_SCDID_QLFR_04_CD,
  COALESCE(B.ASRG_SCDID_QLFR_05_CD,'') AS ASRG_SCDID_QLFR_05_CD,
  COALESCE(B.ASRG_SCDID_QLFR_06_CD,'') AS ASRG_SCDID_QLFR_06_CD,
  COALESCE(B.ASRG_SCDID_QLFR_07_CD,'') AS ASRG_SCDID_QLFR_07_CD,
  COALESCE(B.ASRG_SCDID_QLFR_08_CD,'') AS ASRG_SCDID_QLFR_08_CD,
  COALESCE(B.ASRG_SCDID_QLFR_09_CD,'') AS ASRG_SCDID_QLFR_09_CD,
  COALESCE(B.ASRG_SCDID_QLFR_10_CD,'') AS ASRG_SCDID_QLFR_10_CD,
  COALESCE(B.ASRG_SCDID_QLFR_11_CD,'') AS ASRG_SCDID_QLFR_11_CD,
  COALESCE(B.ASRG_SCDID_QLFR_12_CD,'') AS ASRG_SCDID_QLFR_12_CD,
  COALESCE(B.ASRG_SCDID_QLFR_13_CD,'') AS ASRG_SCDID_QLFR_13_CD,
  COALESCE(B.ASRG_SCDID_QLFR_14_CD,'') AS ASRG_SCDID_QLFR_14_CD,
  COALESCE(B.ASRG_SCDID_QLFR_15_CD,'') AS ASRG_SCDID_QLFR_15_CD,
  COALESCE(B.ASRG_SCDID_QLFR_16_CD,'') AS ASRG_SCDID_QLFR_16_CD,
  COALESCE(B.ASRG_SCDID_QLFR_17_CD,'') AS ASRG_SCDID_QLFR_17_CD,
  COALESCE(B.ASRG_SCDID_QLFR_18_CD,'') AS ASRG_SCDID_QLFR_18_CD,
  COALESCE(B.ASRG_SCDID_QLFR_19_CD,'') AS ASRG_SCDID_QLFR_19_CD,
  COALESCE(B.ASRG_SCDID_QLFR_20_CD,'') AS ASRG_SCDID_QLFR_20_CD,
  COALESCE(B.ASRG_SNDRY_01_ID,'') AS ASRG_SNDRY_01_ID,
  COALESCE(B.ASRG_SNDRY_02_ID,'') AS ASRG_SNDRY_02_ID,
  COALESCE(B.ASRG_SNDRY_03_ID,'') AS ASRG_SNDRY_03_ID,
  COALESCE(B.ASRG_SNDRY_04_ID,'') AS ASRG_SNDRY_04_ID,
  COALESCE(B.ASRG_SNDRY_05_ID,'') AS ASRG_SNDRY_05_ID,
  COALESCE(B.ASRG_SNDRY_06_ID,'') AS ASRG_SNDRY_06_ID,
  COALESCE(B.ASRG_SNDRY_07_ID,'') AS ASRG_SNDRY_07_ID,
  COALESCE(B.ASRG_SNDRY_08_ID,'') AS ASRG_SNDRY_08_ID,
  COALESCE(B.ASRG_SNDRY_09_ID,'') AS ASRG_SNDRY_09_ID,
  COALESCE(B.ASRG_SNDRY_10_ID,'') AS ASRG_SNDRY_10_ID,
  COALESCE(B.ASRG_SNDRY_11_ID,'') AS ASRG_SNDRY_11_ID,
  COALESCE(B.ASRG_SNDRY_12_ID,'') AS ASRG_SNDRY_12_ID,
  COALESCE(B.ASRG_SNDRY_13_ID,'') AS ASRG_SNDRY_13_ID,
  COALESCE(B.ASRG_SNDRY_14_ID,'') AS ASRG_SNDRY_14_ID,
  COALESCE(B.ASRG_SNDRY_15_ID,'') AS ASRG_SNDRY_15_ID,
  COALESCE(B.ASRG_SNDRY_16_ID,'') AS ASRG_SNDRY_16_ID,
  COALESCE(B.ASRG_SNDRY_17_ID,'') AS ASRG_SNDRY_17_ID,
  COALESCE(B.ASRG_SNDRY_18_ID,'') AS ASRG_SNDRY_18_ID,
  COALESCE(B.ASRG_SNDRY_19_ID,'') AS ASRG_SNDRY_19_ID,
  COALESCE(B.ASRG_SNDRY_20_ID,'') AS ASRG_SNDRY_20_ID,
  COALESCE(B.ASRG_TXNMY_CD,'') AS ASRG_TXNMY_CD,
  COALESCE(B.REND_PROV_ENTY_TYP_QLFR_CD,'') AS REND_PROV_ENTY_TYP_QLFR_CD,
  COALESCE(B.REND_PROV_FST_NM,'') AS REND_PROV_FST_NM,
  COALESCE(B.REND_PROV_ID,'') AS REND_PROV_ID,
  COALESCE(B.REND_PROV_ID_QLFR_CD,'') AS REND_PROV_ID_QLFR_CD,
  COALESCE(B.REND_PROV_LST_NM,'') AS REND_PROV_LST_NM,
  COALESCE(B.REND_PROV_MID_NM,'') AS REND_PROV_MID_NM,
  COALESCE(B.REND_PROV_NM_SFX_TXT,'') AS REND_PROV_NM_SFX_TXT,
  COALESCE(B.REND_PROV_OTPY_PRM_01_ID,'') AS REND_PROV_OTPY_PRM_01_ID,
  COALESCE(B.REND_PROV_OTPY_PRM_02_ID,'') AS REND_PROV_OTPY_PRM_02_ID,
  COALESCE(B.REND_PROV_OTPY_PRM_03_ID,'') AS REND_PROV_OTPY_PRM_03_ID,
  COALESCE(B.REND_PROV_OTPY_PRM_04_ID,'') AS REND_PROV_OTPY_PRM_04_ID,
  COALESCE(B.REND_PROV_OTPY_PRM_05_ID,'') AS REND_PROV_OTPY_PRM_05_ID,
  COALESCE(B.REND_PROV_OTPY_PRM_06_ID,'') AS REND_PROV_OTPY_PRM_06_ID,
  COALESCE(B.REND_PROV_OTPY_PRM_07_ID,'') AS REND_PROV_OTPY_PRM_07_ID,
  COALESCE(B.REND_PROV_OTPY_PRM_08_ID,'') AS REND_PROV_OTPY_PRM_08_ID,
  COALESCE(B.REND_PROV_OTPY_PRM_09_ID,'') AS REND_PROV_OTPY_PRM_09_ID,
  COALESCE(B.REND_PROV_OTPY_PRM_10_ID,'') AS REND_PROV_OTPY_PRM_10_ID,
  COALESCE(B.REND_PROV_OTPY_PRM_11_ID,'') AS REND_PROV_OTPY_PRM_11_ID,
  COALESCE(B.REND_PROV_OTPY_PRM_12_ID,'') AS REND_PROV_OTPY_PRM_12_ID,
  COALESCE(B.REND_PROV_OTPY_PRM_13_ID,'') AS REND_PROV_OTPY_PRM_13_ID,
  COALESCE(B.REND_PROV_OTPY_PRM_14_ID,'') AS REND_PROV_OTPY_PRM_14_ID,
  COALESCE(B.REND_PROV_OTPY_PRM_15_ID,'') AS REND_PROV_OTPY_PRM_15_ID,
  COALESCE(B.REND_PROV_OTPY_PRM_16_ID,'') AS REND_PROV_OTPY_PRM_16_ID,
  COALESCE(B.REND_PROV_OTPY_PRM_17_ID,'') AS REND_PROV_OTPY_PRM_17_ID,
  COALESCE(B.REND_PROV_OTPY_PRM_18_ID,'') AS REND_PROV_OTPY_PRM_18_ID,
  COALESCE(B.REND_PROV_OTPY_PRM_19_ID,'') AS REND_PROV_OTPY_PRM_19_ID,
  COALESCE(B.REND_PROV_OTPY_PRM_20_ID,'') AS REND_PROV_OTPY_PRM_20_ID,
  COALESCE(B.REND_PROV_SCDID_QLFR_01_CD,'') AS REND_PROV_SCDID_QLFR_01_CD,
  COALESCE(B.REND_PROV_SCDID_QLFR_02_CD,'') AS REND_PROV_SCDID_QLFR_02_CD,
  COALESCE(B.REND_PROV_SCDID_QLFR_03_CD,'') AS REND_PROV_SCDID_QLFR_03_CD,
  COALESCE(B.REND_PROV_SCDID_QLFR_04_CD,'') AS REND_PROV_SCDID_QLFR_04_CD,
  COALESCE(B.REND_PROV_SCDID_QLFR_05_CD,'') AS REND_PROV_SCDID_QLFR_05_CD,
  COALESCE(B.REND_PROV_SCDID_QLFR_06_CD,'') AS REND_PROV_SCDID_QLFR_06_CD,
  COALESCE(B.REND_PROV_SCDID_QLFR_07_CD,'') AS REND_PROV_SCDID_QLFR_07_CD,
  COALESCE(B.REND_PROV_SCDID_QLFR_08_CD,'') AS REND_PROV_SCDID_QLFR_08_CD,
  COALESCE(B.REND_PROV_SCDID_QLFR_09_CD,'') AS REND_PROV_SCDID_QLFR_09_CD,
  COALESCE(B.REND_PROV_SCDID_QLFR_10_CD,'') AS REND_PROV_SCDID_QLFR_10_CD,
  COALESCE(B.REND_PROV_SCDID_QLFR_11_CD,'') AS REND_PROV_SCDID_QLFR_11_CD,
  COALESCE(B.REND_PROV_SCDID_QLFR_12_CD,'') AS REND_PROV_SCDID_QLFR_12_CD,
  COALESCE(B.REND_PROV_SCDID_QLFR_13_CD,'') AS REND_PROV_SCDID_QLFR_13_CD,
  COALESCE(B.REND_PROV_SCDID_QLFR_14_CD,'') AS REND_PROV_SCDID_QLFR_14_CD,
  COALESCE(B.REND_PROV_SCDID_QLFR_15_CD,'') AS REND_PROV_SCDID_QLFR_15_CD,
  COALESCE(B.REND_PROV_SCDID_QLFR_16_CD,'') AS REND_PROV_SCDID_QLFR_16_CD,
  COALESCE(B.REND_PROV_SCDID_QLFR_17_CD,'') AS REND_PROV_SCDID_QLFR_17_CD,
  COALESCE(B.REND_PROV_SCDID_QLFR_18_CD,'') AS REND_PROV_SCDID_QLFR_18_CD,
  COALESCE(B.REND_PROV_SCDID_QLFR_19_CD,'') AS REND_PROV_SCDID_QLFR_19_CD,
  COALESCE(B.REND_PROV_SCDID_QLFR_20_CD,'') AS REND_PROV_SCDID_QLFR_20_CD,
  COALESCE(B.REND_PROV_SNDRY_01_ID,'') AS REND_PROV_SNDRY_01_ID,
  COALESCE(B.REND_PROV_SNDRY_02_ID,'') AS REND_PROV_SNDRY_02_ID,
  COALESCE(B.REND_PROV_SNDRY_03_ID,'') AS REND_PROV_SNDRY_03_ID,
  COALESCE(B.REND_PROV_SNDRY_04_ID,'') AS REND_PROV_SNDRY_04_ID,
  COALESCE(B.REND_PROV_SNDRY_05_ID,'') AS REND_PROV_SNDRY_05_ID,
  COALESCE(B.REND_PROV_SNDRY_06_ID,'') AS REND_PROV_SNDRY_06_ID,
  COALESCE(B.REND_PROV_SNDRY_07_ID,'') AS REND_PROV_SNDRY_07_ID,
  COALESCE(B.REND_PROV_SNDRY_08_ID,'') AS REND_PROV_SNDRY_08_ID,
  COALESCE(B.REND_PROV_SNDRY_09_ID,'') AS REND_PROV_SNDRY_09_ID,
  COALESCE(B.REND_PROV_SNDRY_10_ID,'') AS REND_PROV_SNDRY_10_ID,
  COALESCE(B.REND_PROV_SNDRY_11_ID,'') AS REND_PROV_SNDRY_11_ID,
  COALESCE(B.REND_PROV_SNDRY_12_ID,'') AS REND_PROV_SNDRY_12_ID,
  COALESCE(B.REND_PROV_SNDRY_13_ID,'') AS REND_PROV_SNDRY_13_ID,
  COALESCE(B.REND_PROV_SNDRY_14_ID,'') AS REND_PROV_SNDRY_14_ID,
  COALESCE(B.REND_PROV_SNDRY_15_ID,'') AS REND_PROV_SNDRY_15_ID,
  COALESCE(B.REND_PROV_SNDRY_16_ID,'') AS REND_PROV_SNDRY_16_ID,
  COALESCE(B.REND_PROV_SNDRY_17_ID,'') AS REND_PROV_SNDRY_17_ID,
  COALESCE(B.REND_PROV_SNDRY_18_ID,'') AS REND_PROV_SNDRY_18_ID,
  COALESCE(B.REND_PROV_SNDRY_19_ID,'') AS REND_PROV_SNDRY_19_ID,
  COALESCE(B.REND_PROV_SNDRY_20_ID,'') AS REND_PROV_SNDRY_20_ID,
  COALESCE(B.REND_PROV_TXNMY_CD,'') AS REND_PROV_TXNMY_CD,
  COALESCE(B.SPVS_SURG_ENTY_TYP_QLFR_CD,'') AS SPVS_SURG_ENTY_TYP_QLFR_CD,
  COALESCE(B.SPVS_SURG_FST_NM,'') AS SPVS_SURG_FST_NM,
  COALESCE(B.SPVS_SURG_ID,'') AS SPVS_SURG_ID,
  COALESCE(B.SPVS_SURG_ID_QLFR_CD,'') AS SPVS_SURG_ID_QLFR_CD,
  COALESCE(B.SPVS_SURG_LST_NM,'') AS SPVS_SURG_LST_NM,
  COALESCE(B.SPVS_SURG_MID_NM,'') AS SPVS_SURG_MID_NM,
  COALESCE(B.SPVS_SURG_NM_SFX_TXT,'') AS SPVS_SURG_NM_SFX_TXT,
  COALESCE(B.SPVS_SURG_OTPY_PRM_01_ID,'') AS SPVS_SURG_OTPY_PRM_01_ID,
  COALESCE(B.SPVS_SURG_OTPY_PRM_02_ID,'') AS SPVS_SURG_OTPY_PRM_02_ID,
  COALESCE(B.SPVS_SURG_OTPY_PRM_03_ID,'') AS SPVS_SURG_OTPY_PRM_03_ID,
  COALESCE(B.SPVS_SURG_OTPY_PRM_04_ID,'') AS SPVS_SURG_OTPY_PRM_04_ID,
  COALESCE(B.SPVS_SURG_OTPY_PRM_05_ID,'') AS SPVS_SURG_OTPY_PRM_05_ID,
  COALESCE(B.SPVS_SURG_OTPY_PRM_06_ID,'') AS SPVS_SURG_OTPY_PRM_06_ID,
  COALESCE(B.SPVS_SURG_OTPY_PRM_07_ID,'') AS SPVS_SURG_OTPY_PRM_07_ID,
  COALESCE(B.SPVS_SURG_OTPY_PRM_08_ID,'') AS SPVS_SURG_OTPY_PRM_08_ID,
  COALESCE(B.SPVS_SURG_OTPY_PRM_09_ID,'') AS SPVS_SURG_OTPY_PRM_09_ID,
  COALESCE(B.SPVS_SURG_OTPY_PRM_10_ID,'') AS SPVS_SURG_OTPY_PRM_10_ID,
  COALESCE(B.SPVS_SURG_OTPY_PRM_11_ID,'') AS SPVS_SURG_OTPY_PRM_11_ID,
  COALESCE(B.SPVS_SURG_OTPY_PRM_12_ID,'') AS SPVS_SURG_OTPY_PRM_12_ID,
  COALESCE(B.SPVS_SURG_OTPY_PRM_13_ID,'') AS SPVS_SURG_OTPY_PRM_13_ID,
  COALESCE(B.SPVS_SURG_OTPY_PRM_14_ID,'') AS SPVS_SURG_OTPY_PRM_14_ID,
  COALESCE(B.SPVS_SURG_OTPY_PRM_15_ID,'') AS SPVS_SURG_OTPY_PRM_15_ID,
  COALESCE(B.SPVS_SURG_OTPY_PRM_16_ID,'') AS SPVS_SURG_OTPY_PRM_16_ID,
  COALESCE(B.SPVS_SURG_OTPY_PRM_17_ID,'') AS SPVS_SURG_OTPY_PRM_17_ID,
  COALESCE(B.SPVS_SURG_OTPY_PRM_18_ID,'') AS SPVS_SURG_OTPY_PRM_18_ID,
  COALESCE(B.SPVS_SURG_OTPY_PRM_19_ID,'') AS SPVS_SURG_OTPY_PRM_19_ID,
  COALESCE(B.SPVS_SURG_OTPY_PRM_20_ID,'') AS SPVS_SURG_OTPY_PRM_20_ID,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_01_CD,'') AS SPVS_SURG_SCDID_QLFR_01_CD,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_02_CD,'') AS SPVS_SURG_SCDID_QLFR_02_CD,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_03_CD,'') AS SPVS_SURG_SCDID_QLFR_03_CD,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_04_CD,'') AS SPVS_SURG_SCDID_QLFR_04_CD,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_05_CD,'') AS SPVS_SURG_SCDID_QLFR_05_CD,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_06_CD,'') AS SPVS_SURG_SCDID_QLFR_06_CD,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_07_CD,'') AS SPVS_SURG_SCDID_QLFR_07_CD,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_08_CD,'') AS SPVS_SURG_SCDID_QLFR_08_CD,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_09_CD,'') AS SPVS_SURG_SCDID_QLFR_09_CD,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_10_CD,'') AS SPVS_SURG_SCDID_QLFR_10_CD,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_11_CD,'') AS SPVS_SURG_SCDID_QLFR_11_CD,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_12_CD,'') AS SPVS_SURG_SCDID_QLFR_12_CD,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_13_CD,'') AS SPVS_SURG_SCDID_QLFR_13_CD,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_14_CD,'') AS SPVS_SURG_SCDID_QLFR_14_CD,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_15_CD,'') AS SPVS_SURG_SCDID_QLFR_15_CD,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_16_CD,'') AS SPVS_SURG_SCDID_QLFR_16_CD,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_17_CD,'') AS SPVS_SURG_SCDID_QLFR_17_CD,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_18_CD,'') AS SPVS_SURG_SCDID_QLFR_18_CD,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_19_CD,'') AS SPVS_SURG_SCDID_QLFR_19_CD,
  COALESCE(B.SPVS_SURG_SCDID_QLFR_20_CD,'') AS SPVS_SURG_SCDID_QLFR_20_CD,
  COALESCE(B.SPVS_SURG_SNDRY_01_ID,'') AS SPVS_SURG_SNDRY_01_ID,
  COALESCE(B.SPVS_SURG_SNDRY_02_ID,'') AS SPVS_SURG_SNDRY_02_ID,
  COALESCE(B.SPVS_SURG_SNDRY_03_ID,'') AS SPVS_SURG_SNDRY_03_ID,
  COALESCE(B.SPVS_SURG_SNDRY_04_ID,'') AS SPVS_SURG_SNDRY_04_ID,
  COALESCE(B.SPVS_SURG_SNDRY_05_ID,'') AS SPVS_SURG_SNDRY_05_ID,
  COALESCE(B.SPVS_SURG_SNDRY_06_ID,'') AS SPVS_SURG_SNDRY_06_ID,
  COALESCE(B.SPVS_SURG_SNDRY_07_ID,'') AS SPVS_SURG_SNDRY_07_ID,
  COALESCE(B.SPVS_SURG_SNDRY_08_ID,'') AS SPVS_SURG_SNDRY_08_ID,
  COALESCE(B.SPVS_SURG_SNDRY_09_ID,'') AS SPVS_SURG_SNDRY_09_ID,
  COALESCE(B.SPVS_SURG_SNDRY_10_ID,'') AS SPVS_SURG_SNDRY_10_ID,
  COALESCE(B.SPVS_SURG_SNDRY_11_ID,'') AS SPVS_SURG_SNDRY_11_ID,
  COALESCE(B.SPVS_SURG_SNDRY_12_ID,'') AS SPVS_SURG_SNDRY_12_ID,
  COALESCE(B.SPVS_SURG_SNDRY_13_ID,'') AS SPVS_SURG_SNDRY_13_ID,
  COALESCE(B.SPVS_SURG_SNDRY_14_ID,'') AS SPVS_SURG_SNDRY_14_ID,
  COALESCE(B.SPVS_SURG_SNDRY_15_ID,'') AS SPVS_SURG_SNDRY_15_ID,
  COALESCE(B.SPVS_SURG_SNDRY_16_ID,'') AS SPVS_SURG_SNDRY_16_ID,
  COALESCE(B.SPVS_SURG_SNDRY_17_ID,'') AS SPVS_SURG_SNDRY_17_ID,
  COALESCE(B.SPVS_SURG_SNDRY_18_ID,'') AS SPVS_SURG_SNDRY_18_ID,
  COALESCE(B.SPVS_SURG_SNDRY_19_ID,'') AS SPVS_SURG_SNDRY_19_ID,
  COALESCE(B.SPVS_SURG_SNDRY_20_ID,'') AS SPVS_SURG_SNDRY_20_ID,
  COALESCE(B.SVC_FAC_LOC_ADDR_LN_01_TXT,'') AS SVC_FAC_LOC_ADDR_LN_01_TXT,
  COALESCE(B.SVC_FAC_LOC_ADDR_LN_02_TXT,'') AS SVC_FAC_LOC_ADDR_LN_02_TXT,
  COALESCE(B.SVC_FAC_LOC_CITY_NM,'') AS SVC_FAC_LOC_CITY_NM,
  COALESCE(B.SVC_FAC_LOC_CNTRY_CD,'') AS SVC_FAC_LOC_CNTRY_CD,
  COALESCE(B.SVC_FAC_LOC_CNTRY_SUBDV_CD,'') AS SVC_FAC_LOC_CNTRY_SUBDV_CD,
  COALESCE(B.SVC_FAC_LOC_ENTY_TYP_QLFR_CD,'') AS SVC_FAC_LOC_ENTY_TYP_QLFR_CD,
  COALESCE(B.SVC_FAC_LOC_ID_QLFR_CD,'') AS SVC_FAC_LOC_ID_QLFR_CD,
  COALESCE(B.SVC_FAC_LOC_LST_NM,'') AS SVC_FAC_LOC_LST_NM,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_01_ID,'') AS SVC_FAC_LOC_OTPY_PRM_01_ID,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_02_ID,'') AS SVC_FAC_LOC_OTPY_PRM_02_ID,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_03_ID,'') AS SVC_FAC_LOC_OTPY_PRM_03_ID,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_04_ID,'') AS SVC_FAC_LOC_OTPY_PRM_04_ID,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_05_ID,'') AS SVC_FAC_LOC_OTPY_PRM_05_ID,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_06_ID,'') AS SVC_FAC_LOC_OTPY_PRM_06_ID,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_07_ID,'') AS SVC_FAC_LOC_OTPY_PRM_07_ID,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_08_ID,'') AS SVC_FAC_LOC_OTPY_PRM_08_ID,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_09_ID,'') AS SVC_FAC_LOC_OTPY_PRM_09_ID,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_10_ID,'') AS SVC_FAC_LOC_OTPY_PRM_10_ID,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_11_ID,'') AS SVC_FAC_LOC_OTPY_PRM_11_ID,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_12_ID,'') AS SVC_FAC_LOC_OTPY_PRM_12_ID,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_13_ID,'') AS SVC_FAC_LOC_OTPY_PRM_13_ID,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_14_ID,'') AS SVC_FAC_LOC_OTPY_PRM_14_ID,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_15_ID,'') AS SVC_FAC_LOC_OTPY_PRM_15_ID,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_16_ID,'') AS SVC_FAC_LOC_OTPY_PRM_16_ID,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_17_ID,'') AS SVC_FAC_LOC_OTPY_PRM_17_ID,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_18_ID,'') AS SVC_FAC_LOC_OTPY_PRM_18_ID,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_19_ID,'') AS SVC_FAC_LOC_OTPY_PRM_19_ID,
  COALESCE(B.SVC_FAC_LOC_OTPY_PRM_20_ID,'') AS SVC_FAC_LOC_OTPY_PRM_20_ID,
  COALESCE(B.SVC_FAC_LOC_PRIM_ID,'') AS SVC_FAC_LOC_PRIM_ID,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_01_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_01_CD,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_02_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_02_CD,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_03_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_03_CD,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_04_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_04_CD,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_05_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_05_CD,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_06_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_06_CD,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_07_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_07_CD,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_08_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_08_CD,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_09_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_09_CD,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_10_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_10_CD,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_11_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_11_CD,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_12_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_12_CD,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_13_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_13_CD,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_14_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_14_CD,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_15_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_15_CD,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_16_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_16_CD,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_17_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_17_CD,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_18_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_18_CD,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_19_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_19_CD,
  COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_20_CD,'') AS SVC_FAC_LOC_SCDID_QLFR_20_CD,
  COALESCE(B.SVC_FAC_LOC_SNDRY_01_ID,'') AS SVC_FAC_LOC_SNDRY_01_ID,
  COALESCE(B.SVC_FAC_LOC_SNDRY_02_ID,'') AS SVC_FAC_LOC_SNDRY_02_ID,
  COALESCE(B.SVC_FAC_LOC_SNDRY_03_ID,'') AS SVC_FAC_LOC_SNDRY_03_ID,
  COALESCE(B.SVC_FAC_LOC_SNDRY_04_ID,'') AS SVC_FAC_LOC_SNDRY_04_ID,
  COALESCE(B.SVC_FAC_LOC_SNDRY_05_ID,'') AS SVC_FAC_LOC_SNDRY_05_ID,
  COALESCE(B.SVC_FAC_LOC_SNDRY_06_ID,'') AS SVC_FAC_LOC_SNDRY_06_ID,
  COALESCE(B.SVC_FAC_LOC_SNDRY_07_ID,'') AS SVC_FAC_LOC_SNDRY_07_ID,
  COALESCE(B.SVC_FAC_LOC_SNDRY_08_ID,'') AS SVC_FAC_LOC_SNDRY_08_ID,
  COALESCE(B.SVC_FAC_LOC_SNDRY_09_ID,'') AS SVC_FAC_LOC_SNDRY_09_ID,
  COALESCE(B.SVC_FAC_LOC_SNDRY_10_ID,'') AS SVC_FAC_LOC_SNDRY_10_ID,
  COALESCE(B.SVC_FAC_LOC_SNDRY_11_ID,'') AS SVC_FAC_LOC_SNDRY_11_ID,
  COALESCE(B.SVC_FAC_LOC_SNDRY_12_ID,'') AS SVC_FAC_LOC_SNDRY_12_ID,
  COALESCE(B.SVC_FAC_LOC_SNDRY_13_ID,'') AS SVC_FAC_LOC_SNDRY_13_ID,
  COALESCE(B.SVC_FAC_LOC_SNDRY_14_ID,'') AS SVC_FAC_LOC_SNDRY_14_ID,
  COALESCE(B.SVC_FAC_LOC_SNDRY_15_ID,'') AS SVC_FAC_LOC_SNDRY_15_ID,
  COALESCE(B.SVC_FAC_LOC_SNDRY_16_ID,'') AS SVC_FAC_LOC_SNDRY_16_ID,
  COALESCE(B.SVC_FAC_LOC_SNDRY_17_ID,'') AS SVC_FAC_LOC_SNDRY_17_ID,
  COALESCE(B.SVC_FAC_LOC_SNDRY_18_ID,'') AS SVC_FAC_LOC_SNDRY_18_ID,
  COALESCE(B.SVC_FAC_LOC_SNDRY_19_ID,'') AS SVC_FAC_LOC_SNDRY_19_ID,
  COALESCE(B.SVC_FAC_LOC_SNDRY_20_ID,'') AS SVC_FAC_LOC_SNDRY_20_ID,
  COALESCE(B.SVC_FAC_LOC_ST_CD,'') AS SVC_FAC_LOC_ST_CD,
  COALESCE(B.SVC_FAC_LOC_ZIP_CD,'') AS SVC_FAC_LOC_ZIP_CD,
  COALESCE(B.SVC_FAC_LOC_ZIP_XTNSN_CD,'') AS SVC_FAC_LOC_ZIP_XTNSN_CD,
  C.CONS_CSV_DENTAL_CLM_HX_ID,
   case when A.STK_UCK_ID <= 9223372036854775807 THEN COALESCE(A.STK_UCK_ID,0) else 0  END  STK_UCK_ID,  
  case when B.STK_REND_PROV_NO <= 9223372036854775807 THEN COALESCE(B.STK_REND_PROV_NO,0) else 0  END  STK_REND_PROV_NO
FROM
  $\{IODS_PUB_BQ_DATASET_ENR\}.CSV_5010_DENTAL_SERVICE_LINE_HX A
LEFT OUTER JOIN
  $\{IODS_PUB_BQ_DATASET_ENR\}.CSV_5010_DENTAL_SERVICE_LINE_PROVIDER_HX B
ON
  A.UCK_ID=B.UCK_ID
  AND A.UCK_ID_PREFIX_CD=B.UCK_ID_PREFIX_CD
  AND A.UCK_ID_SEGMENT_NO=B.UCK_ID_SEGMENT_NO
  AND A.SUBMT_SVC_LN_NO=B.SUBMT_SVC_LN_NO
INNER JOIN
  $\{IODS_PUB_BQ_DATASET_CNS\}.CONS_CSV_DENTAL_CLM_HX C
ON
  A.UCK_ID=C.AK_UCK_ID
  AND A.UCK_ID_PREFIX_CD=C.AK_UCK_ID_PREFIX_CD
  AND A.UCK_ID_SEGMENT_NO=C.AK_UCK_ID_SEGMENT_NO
WHERE
  ((A.LOAD_DATE BETWEEN DATE('$\{CSVDNTL_START_DATE\}')
      AND DATE('$\{CSVDNTL_END_DATE\}'))
    OR (B.LOAD_DATE BETWEEN DATE('$\{CSVDNTL_START_DATE\}')
      AND DATE('$\{CSVDNTL_END_DATE\}')))|3|3|c|@{0|}}
{30001002|XXparameter|table_spec_kind|select|3|2|$|@{0|}}
{30001002|XXparameter|dbms|bigquery|3|9||@{0|}}
{30001002|XXparameter|eme_dataset_mapping|$[[record map_component 0 datasets NULL]]|3|9||@{0|}}
{30001002|XXparameter|read_metadata|/* DML Generated for SQL: SELECT A.UCK_ID AS AK_UCK_ID, A.UCK_ID_PREFIX_CD AS AK_UCK_ID_PREFIX_CD, A.UCK_ID_SEGMENT_NO AS AK_UCK_ID_SEGMENT_NO, A.SUBMT_SVC_LN_NO AS AK_SUBMT_SVC_LN_NO, A.ADJT_REPRC_CLM_NO_TXT, A.APLNC_PLCMT_DT, A.ASSIGNED_LN_NO, A.CLAIM_FORMAT_CD, A.CLM_PRIC_METHODOLOGY_CD, A.CLM_PRIC_PROD_SVC_ID, A.CLM_PRIC_PROD_SVC_ID_QLFR_CD, A.CLM_TYP_CD, A.CONTRACT_AMT, A.CONTRACT_CD, A.CONTRACT_PCT, A.CONTRACT_TYP_CD, A.CONTRACT_VER_ID, A.DIAG_CD_POINTER_01_CD, A.DIAG_CD_POINTER_02_CD, A.DIAG_CD_POINTER_03_CD, A.DIAG_CD_POINTER_04_CD, A.FILE_INFO_01_TXT, A.FILE_INFO_02_TXT, A.FILE_INFO_03_TXT, A.FILE_INFO_04_TXT, A.FILE_INFO_05_TXT, A.FILE_INFO_06_TXT, A.FILE_INFO_07_TXT, A.FILE_INFO_08_TXT, A.FILE_INFO_09_TXT, A.FILE_INFO_10_TXT, A.LN_ITEM_CHG_AMT, A.LN_ITEM_CTL_NO_TXT, A.ORAL_CAVITY_DESIGNATION_01_CD, A.ORAL_CAVITY_DESIGNATION_02_CD, A.ORAL_CAVITY_DESIGNATION_03_CD, A.ORAL_CAVITY_DESIGNATION_04_CD, A.ORAL_CAVITY_DESIGNATION_05_CD, A.OTPY_PRM_01_ID, A.OTPY_PRM_02_ID, A.OTPY_PRM_03_ID, A.OTPY_PRM_04_ID, A.OTPY_PRM_05_ID, A.PLCY_COMPLIANCE_CD, A.POS_CD, A.PRED_OF_BNFT_01_ID, A.PRED_OF_BNFT_02_ID, A.PRED_OF_BNFT_03_ID, A.PRED_OF_BNFT_04_ID, A.PRED_OF_BNFT_05_ID, A.PRED_OTPY_PRM_01_ID, A.PRED_OTPY_PRM_02_ID, A.PRED_OTPY_PRM_03_ID, A.PRED_OTPY_PRM_04_ID, A.PRED_OTPY_PRM_05_ID, A.PROC_CD, A.PROC_CD_DESC, A.PROC_CD_MODIFIER_01_CD, A.PROC_CD_MODIFIER_02_CD, A.PROC_CD_MODIFIER_03_CD, A.PROC_CD_MODIFIER_04_CD, A.PROC_CNT, A.PROD_SVC_ID_QLFR_CD, A.PROSTHESIS_CROWN_OR_INLAY_CD, A.PRR_AUTH_01_ID, A.PRR_AUTH_02_ID, A.PRR_AUTH_03_ID, A.PRR_AUTH_04_ID, A.PRR_AUTH_05_ID, A.PRR_AUTH_OTPY_PRM_01_ID, A.PRR_AUTH_OTPY_PRM_02_ID, A.PRR_AUTH_OTPY_PRM_03_ID, A.PRR_AUTH_OTPY_PRM_04_ID, A.PRR_AUTH_OTPY_PRM_05_ID, A.PRR_AUTH_OTPY_PRMID_QLFR_01_CD, A.PRR_AUTH_OTPY_PRMID_QLFR_02_CD, A.PRR_AUTH_OTPY_PRMID_QLFR_03_CD, A.PRR_AUTH_OTPY_PRMID_QLFR_04_CD, A.PRR_AUTH_OTPY_PRMID_QLFR_05_CD, A.PRR_PLCMT_DT, A.PRR_PLCMT_DT_QLFR_CD, A.REF_NO_01_TXT, A.REF_NO_02_TXT, A.REF_NO_03_TXT, A.REF_NO_04_TXT, A.REF_NO_05_TXT, A.REF_OTPY_PRM_ID_QLFR_01_CD, A.REF_OTPY_PRM_ID_QLFR_02_CD, A.REF_OTPY_PRM_ID_QLFR_03_CD, A.REF_OTPY_PRM_ID_QLFR_04_CD, A.REF_OTPY_PRM_ID_QLFR_05_CD, A.REJECT_RSN_CD, A.REPLCMT_DT, A.REPRC_ALLOWED_AMT, A.REPRC_APPRVD_HCPCS_CD, A.REPRC_APPRVD_SVC_UNT_CNT, A.REPRC_CLM_NO_TXT, A.REPRC_ORG_ID, A.REPRC_PER_DIEM_FLAT_RT_AMT, A.REPRC_SAVING_AMT, A.SALES_TAX_PAID_AMT, A.STK_UCK_ID, A.SVC_DT, A.TERMS_DISCNT_PCT, A.TOOTH_01_CD, A.TOOTH_01_SURFACE_01_CD, A.TOOTH_01_SURFACE_02_CD, A.TOOTH_01_SURFACE_03_CD, A.TOOTH_01_SURFACE_04_CD, A.TOOTH_01_SURFACE_05_CD, A.TOOTH_01_SURFACE_06_CD, A.TOOTH_01_SURFACE_07_CD, A.TOOTH_01_SURFACE_08_CD, A.TOOTH_01_SURFACE_09_CD, A.TOOTH_01_SURFACE_10_CD, A.TOOTH_01_SURFACE_11_CD, A.TOOTH_01_SURFACE_12_CD, A.TOOTH_01_SURFACE_13_CD, A.TOOTH_01_SURFACE_14_CD, A.TOOTH_01_SURFACE_15_CD, A.TOOTH_01_SURFACE_16_CD, A.TOOTH_01_SURFACE_17_CD, A.TOOTH_01_SURFACE_18_CD, A.TOOTH_01_SURFACE_19_CD, A.TOOTH_01_SURFACE_20_CD, A.TOOTH_01_SURFACE_21_CD, A.TOOTH_01_SURFACE_22_CD, A.TOOTH_01_SURFACE_23_CD, A.TOOTH_01_SURFACE_24_CD, A.TOOTH_01_SURFACE_25_CD, A.TOOTH_01_SURFACE_26_CD, A.TOOTH_01_SURFACE_27_CD, A.TOOTH_01_SURFACE_28_CD, A.TOOTH_01_SURFACE_29_CD, A.TOOTH_01_SURFACE_30_CD, A.TOOTH_01_SURFACE_31_CD, A.TOOTH_01_SURFACE_32_CD, A.TOOTH_02_CD, A.TOOTH_02_SURFACE_01_CD, A.TOOTH_02_SURFACE_02_CD, A.TOOTH_02_SURFACE_03_CD, A.TOOTH_02_SURFACE_04_CD, A.TOOTH_02_SURFACE_05_CD, A.TOOTH_02_SURFACE_06_CD, A.TOOTH_02_SURFACE_07_CD, A.TOOTH_02_SURFACE_08_CD, A.TOOTH_02_SURFACE_09_CD, A.TOOTH_02_SURFACE_10_CD, A.TOOTH_02_SURFACE_11_CD, A.TOOTH_02_SURFACE_12_CD, A.TOOTH_02_SURFACE_13_CD, A.TOOTH_02_SURFACE_14_CD, A.TOOTH_02_SURFACE_15_CD, A.TOOTH_02_SURFACE_16_CD, A.TOOTH_02_SURFACE_17_CD, A.TOOTH_02_SURFACE_18_CD, A.TOOTH_02_SURFACE_19_CD, A.TOOTH_02_SURFACE_20_CD, A.TOOTH_02_SURFACE_21_CD, A.TOOTH_02_SURFACE_22_CD, A.TOOTH_02_SURFACE_23_CD, A.TOOTH_02_SURFACE_24_CD, A.TOOTH_02_SURFACE_25_CD, A.TOOTH_02_SURFACE_26_CD, A.TOOTH_02_SURFACE_27_CD, A.TOOTH_02_SURFACE_28_CD, A.TOOTH_02_SURFACE_29_CD, A.TOOTH_02_SURFACE_30_CD, A.TOOTH_02_SURFACE_31_CD, A.TOOTH_02_SURFACE_32_CD, A.TOOTH_03_CD, A.TOOTH_03_SURFACE_01_CD, A.TOOTH_03_SURFACE_02_CD, A.TOOTH_03_SURFACE_03_CD, A.TOOTH_03_SURFACE_04_CD, A.TOOTH_03_SURFACE_05_CD, A.TOOTH_03_SURFACE_06_CD, A.TOOTH_03_SURFACE_07_CD, A.TOOTH_03_SURFACE_08_CD, A.TOOTH_03_SURFACE_09_CD, A.TOOTH_03_SURFACE_10_CD, A.TOOTH_03_SURFACE_11_CD, A.TOOTH_03_SURFACE_12_CD, A.TOOTH_03_SURFACE_13_CD, A.TOOTH_03_SURFACE_14_CD, A.TOOTH_03_SURFACE_15_CD, A.TOOTH_03_SURFACE_16_CD, A.TOOTH_03_SURFACE_17_CD, A.TOOTH_03_SURFACE_18_CD, A.TOOTH_03_SURFACE_19_CD, A.TOOTH_03_SURFACE_20_CD, A.TOOTH_03_SURFACE_21_CD, A.TOOTH_03_SURFACE_22_CD, A.TOOTH_03_SURFACE_23_CD, A.TOOTH_03_SURFACE_24_CD, A.TOOTH_03_SURFACE_25_CD, A.TOOTH_03_SURFACE_26_CD, A.TOOTH_03_SURFACE_27_CD, A.TOOTH_03_SURFACE_28_CD, A.TOOTH_03_SURFACE_29_CD, A.TOOTH_03_SURFACE_30_CD, A.TOOTH_03_SURFACE_31_CD, A.TOOTH_03_SURFACE_32_CD, A.TOOTH_04_CD, A.TOOTH_04_SURFACE_01_CD, A.TOOTH_04_SURFACE_02_CD, A.TOOTH_04_SURFACE_03_CD, A.TOOTH_04_SURFACE_04_CD, A.TOOTH_04_SURFACE_05_CD, A.TOOTH_04_SURFACE_06_CD, A.TOOTH_04_SURFACE_07_CD, A.TOOTH_04_SURFACE_08_CD, A.TOOTH_04_SURFACE_09_CD, A.TOOTH_04_SURFACE_10_CD, A.TOOTH_04_SURFACE_11_CD, A.TOOTH_04_SURFACE_12_CD, A.TOOTH_04_SURFACE_13_CD, A.TOOTH_04_SURFACE_14_CD, A.TOOTH_04_SURFACE_15_CD, A.TOOTH_04_SURFACE_16_CD, A.TOOTH_04_SURFACE_17_CD, A.TOOTH_04_SURFACE_18_CD, A.TOOTH_04_SURFACE_19_CD, A.TOOTH_04_SURFACE_20_CD, A.TOOTH_04_SURFACE_21_CD, A.TOOTH_04_SURFACE_22_CD, A.TOOTH_04_SURFACE_23_CD, A.TOOTH_04_SURFACE_24_CD, A.TOOTH_04_SURFACE_25_CD, A.TOOTH_04_SURFACE_26_CD, A.TOOTH_04_SURFACE_27_CD, A.TOOTH_04_SURFACE_28_CD, A.TOOTH_04_SURFACE_29_CD, A.TOOTH_04_SURFACE_30_CD, A.TOOTH_04_SURFACE_31_CD, A.TOOTH_04_SURFACE_32_CD, A.TOOTH_05_CD, A.TOOTH_05_SURFACE_01_CD, A.TOOTH_05_SURFACE_02_CD, A.TOOTH_05_SURFACE_03_CD, A.TOOTH_05_SURFACE_04_CD, A.TOOTH_05_SURFACE_05_CD, A.TOOTH_05_SURFACE_06_CD, A.TOOTH_05_SURFACE_07_CD, A.TOOTH_05_SURFACE_08_CD, A.TOOTH_05_SURFACE_09_CD, A.TOOTH_05_SURFACE_10_CD, A.TOOTH_05_SURFACE_11_CD, A.TOOTH_05_SURFACE_12_CD, A.TOOTH_05_SURFACE_13_CD, A.TOOTH_05_SURFACE_14_CD, A.TOOTH_05_SURFACE_15_CD, A.TOOTH_05_SURFACE_16_CD, A.TOOTH_05_SURFACE_17_CD, A.TOOTH_05_SURFACE_18_CD, A.TOOTH_05_SURFACE_19_CD, A.TOOTH_05_SURFACE_20_CD, A.TOOTH_05_SURFACE_21_CD, A.TOOTH_05_SURFACE_22_CD, A.TOOTH_05_SURFACE_23_CD, A.TOOTH_05_SURFACE_24_CD, A.TOOTH_05_SURFACE_25_CD, A.TOOTH_05_SURFACE_26_CD, A.TOOTH_05_SURFACE_27_CD, A.TOOTH_05_SURFACE_28_CD, A.TOOTH_05_SURFACE_29_CD, A.TOOTH_05_SURFACE_30_CD, A.TOOTH_05_SURFACE_31_CD, A.TOOTH_05_SURFACE_32_CD, A.TOOTH_06_CD, A.TOOTH_07_CD, A.TOOTH_08_CD, A.TOOTH_09_CD, A.TOOTH_10_CD, A.TOOTH_11_CD, A.TOOTH_12_CD, A.TOOTH_13_CD, A.TOOTH_14_CD, A.TOOTH_15_CD, A.TOOTH_16_CD, A.TOOTH_17_CD, A.TOOTH_18_CD, A.TOOTH_19_CD, A.TOOTH_20_CD, A.TOOTH_21_CD, A.TOOTH_22_CD, A.TOOTH_23_CD, A.TOOTH_24_CD, A.TOOTH_25_CD, A.TOOTH_26_CD, A.TOOTH_27_CD, A.TOOTH_28_CD, A.TOOTH_29_CD, A.TOOTH_30_CD, A.TOOTH_31_CD, A.TOOTH_32_CD, A.TRTMT_END_DT, A.TRTMT_START_DT, A.XCPTN_CD, COALESCE(B.ASRG_ENTY_TYP_QLFR_CD,' ') AS ASRG_ENTY_TYP_QLFR_CD, COALESCE(B.ASRG_FST_NM,' ') AS ASRG_FST_NM, COALESCE(B.ASRG_ID,' ') AS ASRG_ID, COALESCE(B.ASRG_ID_QLFR_CD,' ') AS ASRG_ID_QLFR_CD, COALESCE(B.ASRG_LST_NM,' ') AS ASRG_LST_NM, COALESCE(B.ASRG_MID_NM,' ') AS ASRG_MID_NM, COALESCE(B.ASRG_NM_SFX_TXT,' ') AS ASRG_NM_SFX_TXT, COALESCE(B.ASRG_OTPY_PRM_01_ID,' ') AS ASRG_OTPY_PRM_01_ID, COALESCE(B.ASRG_OTPY_PRM_02_ID,' ') AS ASRG_OTPY_PRM_02_ID, COALESCE(B.ASRG_OTPY_PRM_03_ID,' ') AS ASRG_OTPY_PRM_03_ID, COALESCE(B.ASRG_OTPY_PRM_04_ID,' ') AS ASRG_OTPY_PRM_04_ID, COALESCE(B.ASRG_OTPY_PRM_05_ID,' ') AS ASRG_OTPY_PRM_05_ID, COALESCE(B.ASRG_OTPY_PRM_06_ID,' ') AS ASRG_OTPY_PRM_06_ID, COALESCE(B.ASRG_OTPY_PRM_07_ID,' ') AS ASRG_OTPY_PRM_07_ID, COALESCE(B.ASRG_OTPY_PRM_08_ID,' ') AS ASRG_OTPY_PRM_08_ID, COALESCE(B.ASRG_OTPY_PRM_09_ID,' ') AS ASRG_OTPY_PRM_09_ID, COALESCE(B.ASRG_OTPY_PRM_10_ID,' ') AS ASRG_OTPY_PRM_10_ID, COALESCE(B.ASRG_OTPY_PRM_11_ID,' ') AS ASRG_OTPY_PRM_11_ID, COALESCE(B.ASRG_OTPY_PRM_12_ID,' ') AS ASRG_OTPY_PRM_12_ID, COALESCE(B.ASRG_OTPY_PRM_13_ID,' ') AS ASRG_OTPY_PRM_13_ID, COALESCE(B.ASRG_OTPY_PRM_14_ID,' ') AS ASRG_OTPY_PRM_14_ID, COALESCE(B.ASRG_OTPY_PRM_15_ID,' ') AS ASRG_OTPY_PRM_15_ID, COALESCE(B.ASRG_OTPY_PRM_16_ID,' ') AS ASRG_OTPY_PRM_16_ID, COALESCE(B.ASRG_OTPY_PRM_17_ID,' ') AS ASRG_OTPY_PRM_17_ID, COALESCE(B.ASRG_OTPY_PRM_18_ID,' ') AS ASRG_OTPY_PRM_18_ID, COALESCE(B.ASRG_OTPY_PRM_19_ID,' ') AS ASRG_OTPY_PRM_19_ID, COALESCE(B.ASRG_OTPY_PRM_20_ID,' ') AS ASRG_OTPY_PRM_20_ID, COALESCE(B.ASRG_SCDID_QLFR_01_CD,' ') AS ASRG_SCDID_QLFR_01_CD, COALESCE(B.ASRG_SCDID_QLFR_02_CD,' ') AS ASRG_SCDID_QLFR_02_CD, COALESCE(B.ASRG_SCDID_QLFR_03_CD,' ') AS ASRG_SCDID_QLFR_03_CD, COALESCE(B.ASRG_SCDID_QLFR_04_CD,' ') AS ASRG_SCDID_QLFR_04_CD, COALESCE(B.ASRG_SCDID_QLFR_05_CD,' ') AS ASRG_SCDID_QLFR_05_CD, COALESCE(B.ASRG_SCDID_QLFR_06_CD,' ') AS ASRG_SCDID_QLFR_06_CD, COALESCE(B.ASRG_SCDID_QLFR_07_CD,' ') AS ASRG_SCDID_QLFR_07_CD, COALESCE(B.ASRG_SCDID_QLFR_08_CD,' ') AS ASRG_SCDID_QLFR_08_CD, COALESCE(B.ASRG_SCDID_QLFR_09_CD,' ') AS ASRG_SCDID_QLFR_09_CD, COALESCE(B.ASRG_SCDID_QLFR_10_CD,' ') AS ASRG_SCDID_QLFR_10_CD, COALESCE(B.ASRG_SCDID_QLFR_11_CD,' ') AS ASRG_SCDID_QLFR_11_CD, COALESCE(B.ASRG_SCDID_QLFR_12_CD,' ') AS ASRG_SCDID_QLFR_12_CD, COALESCE(B.ASRG_SCDID_QLFR_13_CD,' ') AS ASRG_SCDID_QLFR_13_CD, COALESCE(B.ASRG_SCDID_QLFR_14_CD,' ') AS ASRG_SCDID_QLFR_14_CD, COALESCE(B.ASRG_SCDID_QLFR_15_CD,' ') AS ASRG_SCDID_QLFR_15_CD, COALESCE(B.ASRG_SCDID_QLFR_16_CD,' ') AS ASRG_SCDID_QLFR_16_CD, COALESCE(B.ASRG_SCDID_QLFR_17_CD,' ') AS ASRG_SCDID_QLFR_17_CD, COALESCE(B.ASRG_SCDID_QLFR_18_CD,' ') AS ASRG_SCDID_QLFR_18_CD, COALESCE(B.ASRG_SCDID_QLFR_19_CD,' ') AS ASRG_SCDID_QLFR_19_CD, COALESCE(B.ASRG_SCDID_QLFR_20_CD,' ') AS ASRG_SCDID_QLFR_20_CD, COALESCE(B.ASRG_SNDRY_01_ID,' ') AS ASRG_SNDRY_01_ID, COALESCE(B.ASRG_SNDRY_02_ID,' ') AS ASRG_SNDRY_02_ID, COALESCE(B.ASRG_SNDRY_03_ID,' ') AS ASRG_SNDRY_03_ID, COALESCE(B.ASRG_SNDRY_04_ID,' ') AS ASRG_SNDRY_04_ID, COALESCE(B.ASRG_SNDRY_05_ID,' ') AS ASRG_SNDRY_05_ID, COALESCE(B.ASRG_SNDRY_06_ID,' ') AS ASRG_SNDRY_06_ID, COALESCE(B.ASRG_SNDRY_07_ID,' ') AS ASRG_SNDRY_07_ID, COALESCE(B.ASRG_SNDRY_08_ID,' ') AS ASRG_SNDRY_08_ID, COALESCE(B.ASRG_SNDRY_09_ID,' ') AS ASRG_SNDRY_09_ID, COALESCE(B.ASRG_SNDRY_10_ID,' ') AS ASRG_SNDRY_10_ID, COALESCE(B.ASRG_SNDRY_11_ID,' ') AS ASRG_SNDRY_11_ID, COALESCE(B.ASRG_SNDRY_12_ID,' ') AS ASRG_SNDRY_12_ID, COALESCE(B.ASRG_SNDRY_13_ID,' ') AS ASRG_SNDRY_13_ID, COALESCE(B.ASRG_SNDRY_14_ID,' ') AS ASRG_SNDRY_14_ID, COALESCE(B.ASRG_SNDRY_15_ID,' ') AS ASRG_SNDRY_15_ID, COALESCE(B.ASRG_SNDRY_16_ID,' ') AS ASRG_SNDRY_16_ID, COALESCE(B.ASRG_SNDRY_17_ID,' ') AS ASRG_SNDRY_17_ID, COALESCE(B.ASRG_SNDRY_18_ID,' ') AS ASRG_SNDRY_18_ID, COALESCE(B.ASRG_SNDRY_19_ID,' ') AS ASRG_SNDRY_19_ID, COALESCE(B.ASRG_SNDRY_20_ID,' ') AS ASRG_SNDRY_20_ID, COALESCE(B.ASRG_TXNMY_CD,' ') AS ASRG_TXNMY_CD, COALESCE(B.REND_PROV_ENTY_TYP_QLFR_CD,' ') AS REND_PROV_ENTY_TYP_QLFR_CD, COALESCE(B.REND_PROV_FST_NM,' ') AS REND_PROV_FST_NM, COALESCE(B.REND_PROV_ID,' ') AS REND_PROV_ID, COALESCE(B.REND_PROV_ID_QLFR_CD,' ') AS REND_PROV_ID_QLFR_CD, COALESCE(B.REND_PROV_LST_NM,' ') AS REND_PROV_LST_NM, COALESCE(B.REND_PROV_MID_NM,' ') AS REND_PROV_MID_NM, COALESCE(B.REND_PROV_NM_SFX_TXT,' ') AS REND_PROV_NM_SFX_TXT, COALESCE(B.REND_PROV_OTPY_PRM_01_ID,' ') AS REND_PROV_OTPY_PRM_01_ID, COALESCE(B.REND_PROV_OTPY_PRM_02_ID,' ') AS REND_PROV_OTPY_PRM_02_ID, COALESCE(B.REND_PROV_OTPY_PRM_03_ID,' ') AS REND_PROV_OTPY_PRM_03_ID, COALESCE(B.REND_PROV_OTPY_PRM_04_ID,' ') AS REND_PROV_OTPY_PRM_04_ID, COALESCE(B.REND_PROV_OTPY_PRM_05_ID,' ') AS REND_PROV_OTPY_PRM_05_ID, COALESCE(B.REND_PROV_OTPY_PRM_06_ID,' ') AS REND_PROV_OTPY_PRM_06_ID, COALESCE(B.REND_PROV_OTPY_PRM_07_ID,' ') AS REND_PROV_OTPY_PRM_07_ID, COALESCE(B.REND_PROV_OTPY_PRM_08_ID,' ') AS REND_PROV_OTPY_PRM_08_ID, COALESCE(B.REND_PROV_OTPY_PRM_09_ID,' ') AS REND_PROV_OTPY_PRM_09_ID, COALESCE(B.REND_PROV_OTPY_PRM_10_ID,' ') AS REND_PROV_OTPY_PRM_10_ID, COALESCE(B.REND_PROV_OTPY_PRM_11_ID,' ') AS REND_PROV_OTPY_PRM_11_ID, COALESCE(B.REND_PROV_OTPY_PRM_12_ID,' ') AS REND_PROV_OTPY_PRM_12_ID, COALESCE(B.REND_PROV_OTPY_PRM_13_ID,' ') AS REND_PROV_OTPY_PRM_13_ID, COALESCE(B.REND_PROV_OTPY_PRM_14_ID,' ') AS REND_PROV_OTPY_PRM_14_ID, COALESCE(B.REND_PROV_OTPY_PRM_15_ID,' ') AS REND_PROV_OTPY_PRM_15_ID, COALESCE(B.REND_PROV_OTPY_PRM_16_ID,' ') AS REND_PROV_OTPY_PRM_16_ID, COALESCE(B.REND_PROV_OTPY_PRM_17_ID,' ') AS REND_PROV_OTPY_PRM_17_ID, COALESCE(B.REND_PROV_OTPY_PRM_18_ID,' ') AS REND_PROV_OTPY_PRM_18_ID, COALESCE(B.REND_PROV_OTPY_PRM_19_ID,' ') AS REND_PROV_OTPY_PRM_19_ID, COALESCE(B.REND_PROV_OTPY_PRM_20_ID,' ') AS REND_PROV_OTPY_PRM_20_ID, COALESCE(B.REND_PROV_SCDID_QLFR_01_CD,' ') AS REND_PROV_SCDID_QLFR_01_CD, COALESCE(B.REND_PROV_SCDID_QLFR_02_CD,' ') AS REND_PROV_SCDID_QLFR_02_CD, COALESCE(B.REND_PROV_SCDID_QLFR_03_CD,' ') AS REND_PROV_SCDID_QLFR_03_CD, COALESCE(B.REND_PROV_SCDID_QLFR_04_CD,' ') AS REND_PROV_SCDID_QLFR_04_CD, COALESCE(B.REND_PROV_SCDID_QLFR_05_CD,' ') AS REND_PROV_SCDID_QLFR_05_CD, COALESCE(B.REND_PROV_SCDID_QLFR_06_CD,' ') AS REND_PROV_SCDID_QLFR_06_CD, COALESCE(B.REND_PROV_SCDID_QLFR_07_CD,' ') AS REND_PROV_SCDID_QLFR_07_CD, COALESCE(B.REND_PROV_SCDID_QLFR_08_CD,' ') AS REND_PROV_SCDID_QLFR_08_CD, COALESCE(B.REND_PROV_SCDID_QLFR_09_CD,' ') AS REND_PROV_SCDID_QLFR_09_CD, COALESCE(B.REND_PROV_SCDID_QLFR_10_CD,' ') AS REND_PROV_SCDID_QLFR_10_CD, COALESCE(B.REND_PROV_SCDID_QLFR_11_CD,' ') AS REND_PROV_SCDID_QLFR_11_CD, COALESCE(B.REND_PROV_SCDID_QLFR_12_CD,' ') AS REND_PROV_SCDID_QLFR_12_CD, COALESCE(B.REND_PROV_SCDID_QLFR_13_CD,' ') AS REND_PROV_SCDID_QLFR_13_CD, COALESCE(B.REND_PROV_SCDID_QLFR_14_CD,' ') AS REND_PROV_SCDID_QLFR_14_CD, COALESCE(B.REND_PROV_SCDID_QLFR_15_CD,' ') AS REND_PROV_SCDID_QLFR_15_CD, COALESCE(B.REND_PROV_SCDID_QLFR_16_CD,' ') AS REND_PROV_SCDID_QLFR_16_CD, COALESCE(B.REND_PROV_SCDID_QLFR_17_CD,' ') AS REND_PROV_SCDID_QLFR_17_CD, COALESCE(B.REND_PROV_SCDID_QLFR_18_CD,' ') AS REND_PROV_SCDID_QLFR_18_CD, COALESCE(B.REND_PROV_SCDID_QLFR_19_CD,' ') AS REND_PROV_SCDID_QLFR_19_CD, COALESCE(B.REND_PROV_SCDID_QLFR_20_CD,' ') AS REND_PROV_SCDID_QLFR_20_CD, COALESCE(B.REND_PROV_SNDRY_01_ID,' ') AS REND_PROV_SNDRY_01_ID, COALESCE(B.REND_PROV_SNDRY_02_ID,' ') AS REND_PROV_SNDRY_02_ID, COALESCE(B.REND_PROV_SNDRY_03_ID,' ') AS REND_PROV_SNDRY_03_ID, COALESCE(B.REND_PROV_SNDRY_04_ID,' ') AS REND_PROV_SNDRY_04_ID, COALESCE(B.REND_PROV_SNDRY_05_ID,' ') AS REND_PROV_SNDRY_05_ID, COALESCE(B.REND_PROV_SNDRY_06_ID,' ') AS REND_PROV_SNDRY_06_ID, COALESCE(B.REND_PROV_SNDRY_07_ID,' ') AS REND_PROV_SNDRY_07_ID, COALESCE(B.REND_PROV_SNDRY_08_ID,' ') AS REND_PROV_SNDRY_08_ID, COALESCE(B.REND_PROV_SNDRY_09_ID,' ') AS REND_PROV_SNDRY_09_ID, COALESCE(B.REND_PROV_SNDRY_10_ID,' ') AS REND_PROV_SNDRY_10_ID, COALESCE(B.REND_PROV_SNDRY_11_ID,' ') AS REND_PROV_SNDRY_11_ID, COALESCE(B.REND_PROV_SNDRY_12_ID,' ') AS REND_PROV_SNDRY_12_ID, COALESCE(B.REND_PROV_SNDRY_13_ID,' ') AS REND_PROV_SNDRY_13_ID, COALESCE(B.REND_PROV_SNDRY_14_ID,' ') AS REND_PROV_SNDRY_14_ID, COALESCE(B.REND_PROV_SNDRY_15_ID,' ') AS REND_PROV_SNDRY_15_ID, COALESCE(B.REND_PROV_SNDRY_16_ID,' ') AS REND_PROV_SNDRY_16_ID, COALESCE(B.REND_PROV_SNDRY_17_ID,' ') AS REND_PROV_SNDRY_17_ID, COALESCE(B.REND_PROV_SNDRY_18_ID,' ') AS REND_PROV_SNDRY_18_ID, COALESCE(B.REND_PROV_SNDRY_19_ID,' ') AS REND_PROV_SNDRY_19_ID, COALESCE(B.REND_PROV_SNDRY_20_ID,' ') AS REND_PROV_SNDRY_20_ID, COALESCE(B.REND_PROV_TXNMY_CD,' ') AS REND_PROV_TXNMY_CD, COALESCE(B.SPVS_SURG_ENTY_TYP_QLFR_CD,' ') AS SPVS_SURG_ENTY_TYP_QLFR_CD, COALESCE(B.SPVS_SURG_FST_NM,' ') AS SPVS_SURG_FST_NM, COALESCE(B.SPVS_SURG_ID,' ') AS SPVS_SURG_ID, COALESCE(B.SPVS_SURG_ID_QLFR_CD,' ') AS SPVS_SURG_ID_QLFR_CD, COALESCE(B.SPVS_SURG_LST_NM,' ') AS SPVS_SURG_LST_NM, COALESCE(B.SPVS_SURG_MID_NM,' ') AS SPVS_SURG_MID_NM, COALESCE(B.SPVS_SURG_NM_SFX_TXT,' ') AS SPVS_SURG_NM_SFX_TXT, COALESCE(B.SPVS_SURG_OTPY_PRM_01_ID,' ') AS SPVS_SURG_OTPY_PRM_01_ID, COALESCE(B.SPVS_SURG_OTPY_PRM_02_ID,' ') AS SPVS_SURG_OTPY_PRM_02_ID, COALESCE(B.SPVS_SURG_OTPY_PRM_03_ID,' ') AS SPVS_SURG_OTPY_PRM_03_ID, COALESCE(B.SPVS_SURG_OTPY_PRM_04_ID,' ') AS SPVS_SURG_OTPY_PRM_04_ID, COALESCE(B.SPVS_SURG_OTPY_PRM_05_ID,' ') AS SPVS_SURG_OTPY_PRM_05_ID, COALESCE(B.SPVS_SURG_OTPY_PRM_06_ID,' ') AS SPVS_SURG_OTPY_PRM_06_ID, COALESCE(B.SPVS_SURG_OTPY_PRM_07_ID,' ') AS SPVS_SURG_OTPY_PRM_07_ID, COALESCE(B.SPVS_SURG_OTPY_PRM_08_ID,' ') AS SPVS_SURG_OTPY_PRM_08_ID, COALESCE(B.SPVS_SURG_OTPY_PRM_09_ID,' ') AS SPVS_SURG_OTPY_PRM_09_ID, COALESCE(B.SPVS_SURG_OTPY_PRM_10_ID,' ') AS SPVS_SURG_OTPY_PRM_10_ID, COALESCE(B.SPVS_SURG_OTPY_PRM_11_ID,' ') AS SPVS_SURG_OTPY_PRM_11_ID, COALESCE(B.SPVS_SURG_OTPY_PRM_12_ID,' ') AS SPVS_SURG_OTPY_PRM_12_ID, COALESCE(B.SPVS_SURG_OTPY_PRM_13_ID,' ') AS SPVS_SURG_OTPY_PRM_13_ID, COALESCE(B.SPVS_SURG_OTPY_PRM_14_ID,' ') AS SPVS_SURG_OTPY_PRM_14_ID, COALESCE(B.SPVS_SURG_OTPY_PRM_15_ID,' ') AS SPVS_SURG_OTPY_PRM_15_ID, COALESCE(B.SPVS_SURG_OTPY_PRM_16_ID,' ') AS SPVS_SURG_OTPY_PRM_16_ID, COALESCE(B.SPVS_SURG_OTPY_PRM_17_ID,' ') AS SPVS_SURG_OTPY_PRM_17_ID, COALESCE(B.SPVS_SURG_OTPY_PRM_18_ID,' ') AS SPVS_SURG_OTPY_PRM_18_ID, COALESCE(B.SPVS_SURG_OTPY_PRM_19_ID,' ') AS SPVS_SURG_OTPY_PRM_19_ID, COALESCE(B.SPVS_SURG_OTPY_PRM_20_ID,' ') AS SPVS_SURG_OTPY_PRM_20_ID, COALESCE(B.SPVS_SURG_SCDID_QLFR_01_CD,' ') AS SPVS_SURG_SCDID_QLFR_01_CD, COALESCE(B.SPVS_SURG_SCDID_QLFR_02_CD,' ') AS SPVS_SURG_SCDID_QLFR_02_CD, COALESCE(B.SPVS_SURG_SCDID_QLFR_03_CD,' ') AS SPVS_SURG_SCDID_QLFR_03_CD, COALESCE(B.SPVS_SURG_SCDID_QLFR_04_CD,' ') AS SPVS_SURG_SCDID_QLFR_04_CD, COALESCE(B.SPVS_SURG_SCDID_QLFR_05_CD,' ') AS SPVS_SURG_SCDID_QLFR_05_CD, COALESCE(B.SPVS_SURG_SCDID_QLFR_06_CD,' ') AS SPVS_SURG_SCDID_QLFR_06_CD, COALESCE(B.SPVS_SURG_SCDID_QLFR_07_CD,' ') AS SPVS_SURG_SCDID_QLFR_07_CD, COALESCE(B.SPVS_SURG_SCDID_QLFR_08_CD,' ') AS SPVS_SURG_SCDID_QLFR_08_CD, COALESCE(B.SPVS_SURG_SCDID_QLFR_09_CD,' ') AS SPVS_SURG_SCDID_QLFR_09_CD, COALESCE(B.SPVS_SURG_SCDID_QLFR_10_CD,' ') AS SPVS_SURG_SCDID_QLFR_10_CD, COALESCE(B.SPVS_SURG_SCDID_QLFR_11_CD,' ') AS SPVS_SURG_SCDID_QLFR_11_CD, COALESCE(B.SPVS_SURG_SCDID_QLFR_12_CD,' ') AS SPVS_SURG_SCDID_QLFR_12_CD, COALESCE(B.SPVS_SURG_SCDID_QLFR_13_CD,' ') AS SPVS_SURG_SCDID_QLFR_13_CD, COALESCE(B.SPVS_SURG_SCDID_QLFR_14_CD,' ') AS SPVS_SURG_SCDID_QLFR_14_CD, COALESCE(B.SPVS_SURG_SCDID_QLFR_15_CD,' ') AS SPVS_SURG_SCDID_QLFR_15_CD, COALESCE(B.SPVS_SURG_SCDID_QLFR_16_CD,' ') AS SPVS_SURG_SCDID_QLFR_16_CD, COALESCE(B.SPVS_SURG_SCDID_QLFR_17_CD,' ') AS SPVS_SURG_SCDID_QLFR_17_CD, COALESCE(B.SPVS_SURG_SCDID_QLFR_18_CD,' ') AS SPVS_SURG_SCDID_QLFR_18_CD, COALESCE(B.SPVS_SURG_SCDID_QLFR_19_CD,' ') AS SPVS_SURG_SCDID_QLFR_19_CD, COALESCE(B.SPVS_SURG_SCDID_QLFR_20_CD,' ') AS SPVS_SURG_SCDID_QLFR_20_CD, COALESCE(B.SPVS_SURG_SNDRY_01_ID,' ') AS SPVS_SURG_SNDRY_01_ID, COALESCE(B.SPVS_SURG_SNDRY_02_ID,' ') AS SPVS_SURG_SNDRY_02_ID, COALESCE(B.SPVS_SURG_SNDRY_03_ID,' ') AS SPVS_SURG_SNDRY_03_ID, COALESCE(B.SPVS_SURG_SNDRY_04_ID,' ') AS SPVS_SURG_SNDRY_04_ID, COALESCE(B.SPVS_SURG_SNDRY_05_ID,' ') AS SPVS_SURG_SNDRY_05_ID, COALESCE(B.SPVS_SURG_SNDRY_06_ID,' ') AS SPVS_SURG_SNDRY_06_ID, COALESCE(B.SPVS_SURG_SNDRY_07_ID,' ') AS SPVS_SURG_SNDRY_07_ID, COALESCE(B.SPVS_SURG_SNDRY_08_ID,' ') AS SPVS_SURG_SNDRY_08_ID, COALESCE(B.SPVS_SURG_SNDRY_09_ID,' ') AS SPVS_SURG_SNDRY_09_ID, COALESCE(B.SPVS_SURG_SNDRY_10_ID,' ') AS SPVS_SURG_SNDRY_10_ID, COALESCE(B.SPVS_SURG_SNDRY_11_ID,' ') AS SPVS_SURG_SNDRY_11_ID, COALESCE(B.SPVS_SURG_SNDRY_12_ID,' ') AS SPVS_SURG_SNDRY_12_ID, COALESCE(B.SPVS_SURG_SNDRY_13_ID,' ') AS SPVS_SURG_SNDRY_13_ID, COALESCE(B.SPVS_SURG_SNDRY_14_ID,' ') AS SPVS_SURG_SNDRY_14_ID, COALESCE(B.SPVS_SURG_SNDRY_15_ID,' ') AS SPVS_SURG_SNDRY_15_ID, COALESCE(B.SPVS_SURG_SNDRY_16_ID,' ') AS SPVS_SURG_SNDRY_16_ID, COALESCE(B.SPVS_SURG_SNDRY_17_ID,' ') AS SPVS_SURG_SNDRY_17_ID, COALESCE(B.SPVS_SURG_SNDRY_18_ID,' ') AS SPVS_SURG_SNDRY_18_ID, COALESCE(B.SPVS_SURG_SNDRY_19_ID,' ') AS SPVS_SURG_SNDRY_19_ID, COALESCE(B.SPVS_SURG_SNDRY_20_ID,' ') AS SPVS_SURG_SNDRY_20_ID, COALESCE(B.STK_REND_PROV_NO,0) AS STK_REND_PROV_NO, COALESCE(B.SVC_FAC_LOC_ADDR_LN_01_TXT,' ') AS SVC_FAC_LOC_ADDR_LN_01_TXT, COALESCE(B.SVC_FAC_LOC_ADDR_LN_02_TXT,' ') AS SVC_FAC_LOC_ADDR_LN_02_TXT, COALESCE(B.SVC_FAC_LOC_CITY_NM,' ') AS SVC_FAC_LOC_CITY_NM, COALESCE(B.SVC_FAC_LOC_CNTRY_CD,' ') AS SVC_FAC_LOC_CNTRY_CD, COALESCE(B.SVC_FAC_LOC_CNTRY_SUBDV_CD,' ') AS SVC_FAC_LOC_CNTRY_SUBDV_CD, COALESCE(B.SVC_FAC_LOC_ENTY_TYP_QLFR_CD,' ') AS SVC_FAC_LOC_ENTY_TYP_QLFR_CD, COALESCE(B.SVC_FAC_LOC_ID_QLFR_CD,' ') AS SVC_FAC_LOC_ID_QLFR_CD, COALESCE(B.SVC_FAC_LOC_LST_NM,' ') AS SVC_FAC_LOC_LST_NM, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_01_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_01_ID, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_02_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_02_ID, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_03_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_03_ID, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_04_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_04_ID, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_05_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_05_ID, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_06_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_06_ID, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_07_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_07_ID, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_08_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_08_ID, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_09_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_09_ID, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_10_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_10_ID, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_11_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_11_ID, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_12_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_12_ID, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_13_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_13_ID, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_14_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_14_ID, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_15_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_15_ID, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_16_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_16_ID, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_17_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_17_ID, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_18_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_18_ID, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_19_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_19_ID, COALESCE(B.SVC_FAC_LOC_OTPY_PRM_20_ID,' ') AS SVC_FAC_LOC_OTPY_PRM_20_ID, COALESCE(B.SVC_FAC_LOC_PRIM_ID,' ') AS SVC_FAC_LOC_PRIM_ID, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_01_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_01_CD, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_02_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_02_CD, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_03_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_03_CD, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_04_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_04_CD, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_05_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_05_CD, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_06_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_06_CD, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_07_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_07_CD, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_08_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_08_CD, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_09_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_09_CD, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_10_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_10_CD, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_11_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_11_CD, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_12_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_12_CD, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_13_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_13_CD, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_14_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_14_CD, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_15_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_15_CD, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_16_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_16_CD, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_17_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_17_CD, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_18_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_18_CD, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_19_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_19_CD, COALESCE(B.SVC_FAC_LOC_SCDID_QLFR_20_CD,' ') AS SVC_FAC_LOC_SCDID_QLFR_20_CD, COALESCE(B.SVC_FAC_LOC_SNDRY_01_ID,' ') AS SVC_FAC_LOC_SNDRY_01_ID, COALESCE(B.SVC_FAC_LOC_SNDRY_02_ID,' ') AS SVC_FAC_LOC_SNDRY_02_ID, COALESCE(B.SVC_FAC_LOC_SNDRY_03_ID,' ') AS SVC_FAC_LOC_SNDRY_03_ID, COALESCE(B.SVC_FAC_LOC_SNDRY_04_ID,' ') AS SVC_FAC_LOC_SNDRY_04_ID, COALESCE(B.SVC_FAC_LOC_SNDRY_05_ID,' ') AS SVC_FAC_LOC_SNDRY_05_ID, COALESCE(B.SVC_FAC_LOC_SNDRY_06_ID,' ') AS SVC_FAC_LOC_SNDRY_06_ID, COALESCE(B.SVC_FAC_LOC_SNDRY_07_ID,' ') AS SVC_FAC_LOC_SNDRY_07_ID, COALESCE(B.SVC_FAC_LOC_SNDRY_08_ID,' ') AS SVC_FAC_LOC_SNDRY_08_ID, COALESCE(B.SVC_FAC_LOC_SNDRY_09_ID,' ') AS SVC_FAC_LOC_SNDRY_09_ID, COALESCE(B.SVC_FAC_LOC_SNDRY_10_ID,' ') AS SVC_FAC_LOC_SNDRY_10_ID, COALESCE(B.SVC_FAC_LOC_SNDRY_11_ID,' ') AS SVC_FAC_LOC_SNDRY_11_ID, COALESCE(B.SVC_FAC_LOC_SNDRY_12_ID,' ') AS SVC_FAC_LOC_SNDRY_12_ID, COALESCE(B.SVC_FAC_LOC_SNDRY_13_ID,' ') AS SVC_FAC_LOC_SNDRY_13_ID, COALESCE(B.SVC_FAC_LOC_SNDRY_14_ID,' ') AS SVC_FAC_LOC_SNDRY_14_ID, COALESCE(B.SVC_FAC_LOC_SNDRY_15_ID,' ') AS SVC_FAC_LOC_SNDRY_15_ID, COALESCE(B.SVC_FAC_LOC_SNDRY_16_ID,' ') AS SVC_FAC_LOC_SNDRY_16_ID, COALESCE(B.SVC_FAC_LOC_SNDRY_17_ID,' ') AS SVC_FAC_LOC_SNDRY_17_ID, COALESCE(B.SVC_FAC_LOC_SNDRY_18_ID,' ') AS SVC_FAC_LOC_SNDRY_18_ID, COALESCE(B.SVC_FAC_LOC_SNDRY_19_ID,' ') AS SVC_FAC_LOC_SNDRY_19_ID, COALESCE(B.SVC_FAC_LOC_SNDRY_20_ID,' ') AS SVC_FAC_LOC_SNDRY_20_ID, COALESCE(B.SVC_FAC_LOC_ST_CD,' ') AS SVC_FAC_LOC_ST_CD, COALESCE(B.SVC_FAC_LOC_ZIP_CD,' ') AS SVC_FAC_LOC_ZIP_CD, COALESCE(B.SVC_FAC_LOC_ZIP_XTNSN_CD,' ') AS SVC_FAC_LOC_ZIP_XTNSN_CD, C.CONS_CSV_DENTAL_CLM_HX_ID FROM edp-dev-hcbstorage.edp_hcb_iods_enr.CSV_5010_DENTAL_SERVICE_LINE_HX A LEFT OUTER JOIN edp-dev-hcbstorage.edp_hcb_iods_enr.CSV_5010_DENTAL_SERVICE_LINE_PROVIDER_HX B ON A.UCK_ID=B.UCK_ID AND A.UCK_ID_PREFIX_CD=B.UCK_ID_PREFIX_CD AND A.UCK_ID_SEGMENT_NO=B.UCK_ID_SEGMENT_NO AND A.SUBMT_SVC_LN_NO=B.SUBMT_SVC_LN_NO INNER JOIN edp-dev-hcbstorage.edp_hcb_iods_cnssrc.CONS_CSV_DENTAL_CLM_HX C ON A.UCK_ID=C.AK_UCK_ID AND A.UCK_ID_PREFIX_CD=C.AK_UCK_ID_PREFIX_CD AND A.UCK_ID_SEGMENT_NO=C.AK_UCK_ID_SEGMENT_NO
 * On: Sat Aug 24 02:32:57 2024
 */
record
  utf8 string(unsigned integer(4)) AK_UCK_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) AK_UCK_ID_PREFIX_CD = NULL /*STRING*/;
  integer(8) AK_UCK_ID_SEGMENT_NO = NULL /*INT64*/;
  integer(8) AK_SUBMT_SVC_LN_NO = NULL /*INT64*/;
  utf8 string(unsigned integer(4)) ADJT_REPRC_CLM_NO_TXT = NULL /*STRING*/;
  date("YYYY-MM-DD") APLNC_PLCMT_DT = NULL /*DATE*/;
  integer(8) ASSIGNED_LN_NO = NULL /*INT64*/;
  utf8 string(unsigned integer(4)) CLAIM_FORMAT_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) CLM_PRIC_METHODOLOGY_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) CLM_PRIC_PROD_SVC_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) CLM_PRIC_PROD_SVC_ID_QLFR_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) CLM_TYP_CD = NULL /*STRING*/;
  decimal(40.9, sign_reserved) CONTRACT_AMT = NULL /*NUMERIC*/;
  utf8 string(unsigned integer(4)) CONTRACT_CD = NULL /*STRING*/;
  decimal(40.9, sign_reserved) CONTRACT_PCT = NULL /*NUMERIC*/;
  utf8 string(unsigned integer(4)) CONTRACT_TYP_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) CONTRACT_VER_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) DIAG_CD_POINTER_01_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) DIAG_CD_POINTER_02_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) DIAG_CD_POINTER_03_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) DIAG_CD_POINTER_04_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) FILE_INFO_01_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) FILE_INFO_02_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) FILE_INFO_03_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) FILE_INFO_04_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) FILE_INFO_05_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) FILE_INFO_06_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) FILE_INFO_07_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) FILE_INFO_08_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) FILE_INFO_09_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) FILE_INFO_10_TXT = NULL /*STRING*/;
  decimal(40.9, sign_reserved) LN_ITEM_CHG_AMT = NULL /*NUMERIC*/;
  utf8 string(unsigned integer(4)) LN_ITEM_CTL_NO_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ORAL_CAVITY_DESIGNATION_01_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ORAL_CAVITY_DESIGNATION_02_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ORAL_CAVITY_DESIGNATION_03_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ORAL_CAVITY_DESIGNATION_04_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ORAL_CAVITY_DESIGNATION_05_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) OTPY_PRM_01_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) OTPY_PRM_02_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) OTPY_PRM_03_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) OTPY_PRM_04_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) OTPY_PRM_05_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PLCY_COMPLIANCE_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) POS_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRED_OF_BNFT_01_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRED_OF_BNFT_02_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRED_OF_BNFT_03_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRED_OF_BNFT_04_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRED_OF_BNFT_05_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRED_OTPY_PRM_01_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRED_OTPY_PRM_02_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRED_OTPY_PRM_03_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRED_OTPY_PRM_04_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRED_OTPY_PRM_05_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PROC_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PROC_CD_DESC = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PROC_CD_MODIFIER_01_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PROC_CD_MODIFIER_02_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PROC_CD_MODIFIER_03_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PROC_CD_MODIFIER_04_CD = NULL /*STRING*/;
  decimal(40.9, sign_reserved) PROC_CNT = NULL /*NUMERIC*/;
  utf8 string(unsigned integer(4)) PROD_SVC_ID_QLFR_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PROSTHESIS_CROWN_OR_INLAY_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRR_AUTH_01_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRR_AUTH_02_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRR_AUTH_03_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRR_AUTH_04_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRR_AUTH_05_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRR_AUTH_OTPY_PRM_01_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRR_AUTH_OTPY_PRM_02_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRR_AUTH_OTPY_PRM_03_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRR_AUTH_OTPY_PRM_04_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRR_AUTH_OTPY_PRM_05_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRR_AUTH_OTPY_PRMID_QLFR_01_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRR_AUTH_OTPY_PRMID_QLFR_02_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRR_AUTH_OTPY_PRMID_QLFR_03_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRR_AUTH_OTPY_PRMID_QLFR_04_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) PRR_AUTH_OTPY_PRMID_QLFR_05_CD = NULL /*STRING*/;
  date("YYYY-MM-DD") PRR_PLCMT_DT = NULL /*DATE*/;
  utf8 string(unsigned integer(4)) PRR_PLCMT_DT_QLFR_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REF_NO_01_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REF_NO_02_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REF_NO_03_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REF_NO_04_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REF_NO_05_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REF_OTPY_PRM_ID_QLFR_01_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REF_OTPY_PRM_ID_QLFR_02_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REF_OTPY_PRM_ID_QLFR_03_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REF_OTPY_PRM_ID_QLFR_04_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REF_OTPY_PRM_ID_QLFR_05_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REJECT_RSN_CD = NULL /*STRING*/;
  date("YYYY-MM-DD") REPLCMT_DT = NULL /*DATE*/;
  decimal(40.9, sign_reserved) REPRC_ALLOWED_AMT = NULL /*NUMERIC*/;
  utf8 string(unsigned integer(4)) REPRC_APPRVD_HCPCS_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REPRC_APPRVD_SVC_UNT_CNT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REPRC_CLM_NO_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REPRC_ORG_ID = NULL /*STRING*/;
  decimal(40.9, sign_reserved) REPRC_PER_DIEM_FLAT_RT_AMT = NULL /*NUMERIC*/;
  decimal(40.9, sign_reserved) REPRC_SAVING_AMT = NULL /*NUMERIC*/;
  decimal(40.9, sign_reserved) SALES_TAX_PAID_AMT = NULL /*NUMERIC*/;
  integer(8) STK_UCK_ID = NULL /*INT64*/;
  date("YYYY-MM-DD") SVC_DT = NULL /*DATE*/;
  decimal(40.9, sign_reserved) TERMS_DISCNT_PCT = NULL /*NUMERIC*/;
  utf8 string(unsigned integer(4)) TOOTH_01_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_01_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_02_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_03_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_04_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_05_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_06_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_07_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_08_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_09_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_10_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_11_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_12_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_13_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_14_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_15_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_16_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_17_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_18_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_19_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_20_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_21_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_22_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_23_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_24_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_25_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_26_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_27_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_28_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_29_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_30_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_31_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_01_SURFACE_32_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_01_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_02_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_03_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_04_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_05_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_06_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_07_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_08_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_09_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_10_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_11_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_12_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_13_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_14_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_15_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_16_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_17_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_18_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_19_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_20_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_21_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_22_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_23_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_24_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_25_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_26_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_27_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_28_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_29_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_30_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_31_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_02_SURFACE_32_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_01_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_02_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_03_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_04_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_05_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_06_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_07_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_08_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_09_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_10_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_11_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_12_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_13_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_14_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_15_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_16_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_17_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_18_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_19_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_20_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_21_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_22_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_23_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_24_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_25_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_26_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_27_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_28_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_29_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_30_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_31_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_03_SURFACE_32_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_01_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_02_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_03_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_04_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_05_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_06_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_07_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_08_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_09_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_10_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_11_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_12_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_13_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_14_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_15_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_16_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_17_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_18_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_19_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_20_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_21_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_22_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_23_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_24_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_25_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_26_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_27_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_28_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_29_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_30_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_31_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_04_SURFACE_32_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_01_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_02_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_03_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_04_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_05_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_06_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_07_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_08_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_09_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_10_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_11_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_12_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_13_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_14_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_15_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_16_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_17_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_18_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_19_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_20_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_21_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_22_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_23_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_24_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_25_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_26_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_27_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_28_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_29_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_30_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_31_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_05_SURFACE_32_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_06_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_07_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_08_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_09_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_10_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_11_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_12_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_13_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_14_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_15_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_16_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_17_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_18_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_19_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_20_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_21_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_22_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_23_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_24_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_25_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_26_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_27_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_28_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_29_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_30_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_31_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) TOOTH_32_CD = NULL /*STRING*/;
  date("YYYY-MM-DD") TRTMT_END_DT = NULL /*DATE*/;
  date("YYYY-MM-DD") TRTMT_START_DT = NULL /*DATE*/;
  utf8 string(unsigned integer(4)) XCPTN_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_ENTY_TYP_QLFR_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_FST_NM = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_ID_QLFR_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_LST_NM = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_MID_NM = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_NM_SFX_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_01_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_02_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_03_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_04_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_05_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_06_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_07_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_08_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_09_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_10_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_11_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_12_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_13_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_14_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_15_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_16_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_17_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_18_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_19_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_OTPY_PRM_20_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_01_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_02_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_03_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_04_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_05_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_06_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_07_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_08_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_09_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_10_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_11_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_12_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_13_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_14_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_15_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_16_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_17_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_18_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_19_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SCDID_QLFR_20_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_01_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_02_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_03_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_04_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_05_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_06_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_07_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_08_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_09_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_10_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_11_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_12_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_13_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_14_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_15_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_16_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_17_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_18_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_19_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_SNDRY_20_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) ASRG_TXNMY_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_ENTY_TYP_QLFR_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_FST_NM = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_ID_QLFR_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_LST_NM = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_MID_NM = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_NM_SFX_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_01_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_02_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_03_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_04_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_05_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_06_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_07_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_08_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_09_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_10_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_11_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_12_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_13_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_14_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_15_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_16_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_17_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_18_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_19_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_OTPY_PRM_20_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_01_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_02_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_03_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_04_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_05_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_06_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_07_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_08_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_09_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_10_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_11_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_12_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_13_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_14_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_15_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_16_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_17_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_18_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_19_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SCDID_QLFR_20_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_01_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_02_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_03_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_04_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_05_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_06_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_07_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_08_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_09_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_10_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_11_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_12_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_13_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_14_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_15_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_16_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_17_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_18_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_19_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_SNDRY_20_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) REND_PROV_TXNMY_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_ENTY_TYP_QLFR_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_FST_NM = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_ID_QLFR_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_LST_NM = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_MID_NM = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_NM_SFX_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_01_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_02_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_03_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_04_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_05_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_06_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_07_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_08_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_09_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_10_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_11_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_12_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_13_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_14_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_15_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_16_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_17_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_18_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_19_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_OTPY_PRM_20_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_01_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_02_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_03_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_04_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_05_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_06_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_07_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_08_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_09_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_10_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_11_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_12_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_13_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_14_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_15_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_16_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_17_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_18_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_19_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SCDID_QLFR_20_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_01_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_02_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_03_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_04_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_05_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_06_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_07_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_08_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_09_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_10_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_11_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_12_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_13_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_14_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_15_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_16_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_17_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_18_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_19_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SPVS_SURG_SNDRY_20_ID = NULL /*STRING*/;
  integer(8) STK_REND_PROV_NO = NULL /*INT64*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_ADDR_LN_01_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_ADDR_LN_02_TXT = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_CITY_NM = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_CNTRY_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_CNTRY_SUBDV_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_ENTY_TYP_QLFR_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_ID_QLFR_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_LST_NM = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_01_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_02_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_03_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_04_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_05_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_06_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_07_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_08_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_09_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_10_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_11_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_12_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_13_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_14_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_15_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_16_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_17_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_18_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_19_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_OTPY_PRM_20_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_PRIM_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_01_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_02_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_03_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_04_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_05_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_06_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_07_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_08_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_09_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_10_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_11_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_12_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_13_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_14_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_15_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_16_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_17_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_18_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_19_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SCDID_QLFR_20_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_01_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_02_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_03_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_04_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_05_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_06_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_07_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_08_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_09_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_10_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_11_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_12_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_13_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_14_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_15_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_16_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_17_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_18_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_19_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_SNDRY_20_ID = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_ST_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_ZIP_CD = NULL /*STRING*/;
  utf8 string(unsigned integer(4)) SVC_FAC_LOC_ZIP_XTNSN_CD = NULL /*STRING*/;
  integer(8) CONS_CSV_DENTAL_CLM_HX_ID = NULL /*INT64*/;
end|3|1|l|@{0|}}
{30001002|XXparameter|bigquery_interface|storage_extract|3|1|l|@{0|}}
{30001002|XXparameter|Layout|mfile:dynamic:$PROC_DEPTH|3|2|$|@{0|}}
}}@0|Input Table|200000|126000|0|0|0|0|139|V351S3P1 CSV 5010 DNTL CLMDTL|Ab Initio Software|Built-in 2.2:|1|100|0||6||4097|}}
{2010203004|XXGoport|50|0|117|0|{@{}@0|0|0|0|read|0.0|@@@2448|0|}}
{2010203004|XXGoport|51|0|120|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010505005|XXGtvertex|52|0|122|0|{Unloads data records from a database.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mode|0x0001|1|2|FH$|modes of access|{0|}}
{30001002|XXparameter|HelpID|HICOMP_INPUT_TABLE|3|2|H$||{0|}}
{30001002|XXparameter|config_file||22|2|RFf$||{0|}}
{30001002|XXparameter|table_spec||32|3|RFc||{0|}}
{30001002|XXparameter|table_spec_kind|table|15|1|RFl||{3|table|insert|select|}}
{30001002|XXparameter|db_is_ole|value dbms mssql True value dbms msjet True value dbms msaccess True value dbms oledb True default constant False|13|13|v||{0|}}
{30001002|XXparameter|ora_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|ora_interface.condition|param_exact dbms oracle mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ter_interface_new|FastExport|15|1|Fl|Unload data via api or utility|{5|api|FastExport|PT_export|PT_selector|PT_api_export|}}
{30001002|XXparameter|ter_interface_new.condition|param_exact dbms teradata mode input version 2.12:|3|15|P?|@{0|}}
{30001002|XXparameter|ter_interface|sameas ter_interface_new|3|13|v||{0|}}
{30001002|XXparameter|ter_interface.condition|param_exact dbms teradata mode input|3|15|P?|@{0|}}
{30001002|XXparameter|db2eee_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|db2eee_interface.condition|param_exact dbms db2eee mode input|3|15|P?|@{0|}}
{30001002|XXparameter|db2ee_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|db2ee_interface.condition|param_exact dbms db2ee mode input|3|15|P?|@{0|}}
{30001002|XXparameter|db2_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|db2_interface.condition|param_exact dbms db2 mode input version 3.2.4.r21:|3|15|P?|@{0|}}
{30001002|XXparameter|db2uss_interface_old|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|db2uss_interface_old.condition|param_exact dbms db2uss mode input version :2.12.2.m99|3|15|P?|@{0|}}
{30001002|XXparameter|db2uss_interface|api|15|1|Fl|Unload data via api or utility|{2|api|utility|}}
{30001002|XXparameter|db2uss_interface.condition|param_exact dbms db2uss mode input version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|db2uss_interface_impl|sameas db2uss_interface sameas db2uss_interface_old|3|13|v||{0|}}
{30001002|XXparameter|db2uss_interface_impl.condition|param_exact dbms db2uss mode input|3|15|P?|@{0|}}
{30001002|XXparameter|mssql_interface|api|15|1|Fl|Unload data via api or staged|{2|api|staged|}}
{30001002|XXparameter|mssql_interface.condition|param_exact dbms mssql mode input|3|15|P?|@{0|}}
{30001002|XXparameter|msjet_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|msjet_interface.condition|param_exact dbms msjet mode input|3|15|P?|@{0|}}
{30001002|XXparameter|msaccess_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|msaccess_interface.condition|param_exact dbms msaccess mode input|3|15|P?|@{0|}}
{30001002|XXparameter|oledb_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|oledb_interface.condition|param_exact dbms oledb mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ole_interface|sameas mssql_interface sameas msjet_interface sameas msaccess_interface sameas oledb_interface|3|13|v||{0|}}
{30001002|XXparameter|ole_interface.condition|param_exact db_is_ole True mode input|3|15|P?|@{0|}}
{30001002|XXparameter|odbc_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|odbc_interface.condition|param_exact dbms odbc mode input|3|15|P?|@{0|}}
{30001002|XXparameter|greenplum_interface|api|15|1|Fl|Unload data via api|{1|api|}}
{30001002|XXparameter|greenplum_interface.condition|param_exact dbms greenplum mode input|3|15|P?|@{0|}}
{30001002|XXparameter|postgresql_interface|api|15|1|Fl|Unload data via api|{2|api|copy|}}
{30001002|XXparameter|postgresql_interface.condition|param_exact dbms postgresql mode input|3|15|P?|@{0|}}
{30001002|XXparameter|psqlodbc_interface|api|15|1|Fl|Unload data via api|{1|api|}}
{30001002|XXparameter|psqlodbc_interface.condition|param_exact dbms psqlodbc mode input|3|15|P?|@{0|}}
{30001002|XXparameter|redshift_interface|api|15|1|Fl|Unload data via api or utility|{2|api|unload|}}
{30001002|XXparameter|redshift_interface.condition|param_exact dbms redshift mode input|3|15|P?|@{0|}}
{30001002|XXparameter|snow_interface|api|15|1|Fl|Unload data via api or utility|{2|api|staged_unload|}}
{30001002|XXparameter|snow_interface.condition|param_exact dbms snowflake mode input|3|15|P?|@{0|}}
{30001002|XXparameter|sybase_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|sybase_interface.condition|param_exact dbms sybase mode input|3|15|P?|@{0|}}
{30001002|XXparameter|sybaseiq_interface|api|15|1|Fl|Unload data via api or utility|{2|api|utility|}}
{30001002|XXparameter|sybaseiq_interface.condition|param_exact dbms sybaseiq mode input|3|15|P?|@{0|}}
{30001002|XXparameter|informix_interface|api|15|1|Fl|Unload data via api or utility|{4|api|dbaccess_serial|dbaccess_xps|pload|}}
{30001002|XXparameter|informix_interface.condition|param_exact dbms informix mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ims_interface_old|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|ims_interface_old.condition|param_exact dbms ims mode input version :3.1.6.r26|3|15|P?|@{0|}}
{30001002|XXparameter|ims_interface|api|15|1|Fl|Unload data via api (local) or via IMS connect(remote) |{2|api|ims_connect|}}
{30001002|XXparameter|ims_interface.condition|param_exact dbms ims mode input version 3.1.6.r27:|3|15|P?|@{0|}}
{30001002|XXparameter|adabas_interface|api|15|1|Fl|Unload data via api or from backup tape|{2|api|backup_tape|}}
{30001002|XXparameter|adabas_interface.condition|param_exact dbms adabas mode input|3|15|P?|@{0|}}
{30001002|XXparameter|mongo_interface|api|15|1|Fl|Unload data via api |{1|api|}}
{30001002|XXparameter|mongo_interface.condition|param_exact dbms mongo mode input version 3.2.3.r18.0:|3|15|P?|@{0|}}
{30001002|XXparameter|hbase_interface|api|15|1|Fl|Unload data via api |{1|api|}}
{30001002|XXparameter|hbase_interface.condition|param_exact dbms hbase mode input version 3.2.5.r24:|3|15|P?|@{0|}}
{30001002|XXparameter|cassandra_interface|api|15|1|Fl|Unload data via api |{1|api|}}
{30001002|XXparameter|cassandra_interface.condition|param_exact dbms cassandra mode input version 3.3.2.r8:|3|15|P?|@{0|}}
{30001002|XXparameter|nza_interface_old|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|nza_interface_old.condition|param_exact dbms netezza mode input version :2.15.7.m99|3|15|P?|@{0|}}
{30001002|XXparameter|nza_interface|api|15|1|Fl|Unload data via api or remote external table|{2|api|external_table|}}
{30001002|XXparameter|nza_interface.condition|param_exact dbms netezza mode input version 2.15.8:|3|15|P?|@{0|}}
{30001002|XXparameter|nza_interface_impl|sameas nza_interface sameas nza_interface_old|3|13|v||{0|}}
{30001002|XXparameter|nza_interface_impl.condition|param_exact dbms netezza mode input|3|15|P?|@{0|}}
{30001002|XXparameter|db2_400_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|db2_400_interface.condition|param_exact dbms db2_400 mode input|3|15|P?|@{0|}}
{30001002|XXparameter|odbc_400_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|odbc_400_interface.condition|param_exact dbms odbc_400 mode input|3|15|P?|@{0|}}
{30001002|XXparameter|aster_interface|api|15|1|Fl|Unload data via api or utility|{2|api|ncluster_export|}}
{30001002|XXparameter|aster_interface.condition|param_exact dbms aster mode input|3|15|P?|@{0|}}
{30001002|XXparameter|hana_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|hana_interface.condition|param_exact dbms hana mode input version 3.2.5.r24:|3|15|P?|@{0|}}
{30001002|XXparameter|jdbc_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|jdbc_interface.condition|param_exact dbms jdbc mode input version 3.3.2.r8:|3|15|P?|@{0|}}
{30001002|XXparameter|hive_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|hive_interface.condition|param_exact dbms hive mode input version 3.3.4:|3|15|P?|@{0|}}
{30001002|XXparameter|impala_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|impala_interface.condition|param_exact dbms impala mode input version 3.3.5.r19:|3|15|P?|@{0|}}
{30001002|XXparameter|bigquery_interface|utility|15|1|RFl|Unload data via api, utility, or storage_extract|{3|api|utility|storage_extract|}}
{30001002|XXparameter|bigquery_interface.condition|param_exact dbms bigquery mode input version 4.1.1.r9:|3|15|P?|@{0|}}
{30001002|XXparameter|exasol_interface|utility|15|1|RFl|Unload data via api or utility|{2|api|utility|}}
{30001002|XXparameter|exasol_interface.condition|param_exact dbms exasol mode input version 4.1.4.r20:|3|15|P?|@{0|}}
{30001002|XXparameter|ora__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|ora__interface.condition|param_exact dbms oracle mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ter__interface|Multiload|15|1|Fl|Load data using api or utility|{11|api|Fastload|Multiload|Tpump|PT_load|PT_update|PT_stream|PT_inserter|PT_api_load|PT_api_update|PT_api_stream|}}
{30001002|XXparameter|ter__interface.condition|param_exact dbms teradata mode output|3|15|P?|@{0|}}
{30001002|XXparameter|db2eee__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|db2eee__interface.condition|param_exact dbms db2eee mode output|3|15|P?|@{0|}}
{30001002|XXparameter|db2ee__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|db2ee__interface.condition|param_exact dbms db2ee mode output|3|15|P?|@{0|}}
{30001002|XXparameter|db2__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|db2__interface.condition|param_exact dbms db2 mode output version 3.2.4.r21:|3|15|P?|@{0|}}
{30001002|XXparameter|db2uss__interface_old|api|15|1|Fl|Load data via api or utility|{1|api|}}
{30001002|XXparameter|db2uss__interface_old.condition|param_exact dbms db2uss mode output version :2.12.2.m99|3|15|P?|@{0|}}
{30001002|XXparameter|db2uss__interface|api|15|1|Fl|Load data via api or utility|{2|api|utility|}}
{30001002|XXparameter|db2uss__interface.condition|param_exact dbms db2uss mode output version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|db2uss__interface_impl|sameas db2uss__interface sameas db2uss__interface_old|3|13|v||{0|}}
{30001002|XXparameter|db2uss__interface_impl.condition|param_exact dbms db2uss mode output|3|15|P?|@{0|}}
{30001002|XXparameter|mssql__interface|utility|15|1|Fl|Load data using api, utility, bulkload or staged|{4|api|utility|bulkload|staged|}}
{30001002|XXparameter|mssql__interface.condition|param_exact dbms mssql mode output|3|15|P?|@{0|}}
{30001002|XXparameter|mssql__interface_bad_choice||3|2|RF$|Do not use bulkload in versions before V2-14-53|{0|}}
{30001002|XXparameter|mssql__interface_bad_choice.condition|param_exact dbms mssql mode output param_exact mssql__interface bulkload version :2.14.52|3|15|P?|@{0|}}
{30001002|XXparameter|msjet__interface|api|15|1|Fl|Load data using api or utility|{1|api|}}
{30001002|XXparameter|msjet__interface.condition|param_exact dbms msjet mode output|3|15|P?|@{0|}}
{30001002|XXparameter|msaccess__interface|api|15|1|Fl|Load data using api or utility|{1|api|}}
{30001002|XXparameter|msaccess__interface.condition|param_exact dbms msaccess mode output|3|15|P?|@{0|}}
{30001002|XXparameter|oledb__interface|api|15|1|Fl|Load data using api or bulkload|{2|api|bulkload|}}
{30001002|XXparameter|oledb__interface.condition|param_exact dbms oledb mode output|3|15|P?|@{0|}}
{30001002|XXparameter|oledb__interface_bad_choice||3|2|RF$|Do not use bulkload in versions before V2-14-53|{0|}}
{30001002|XXparameter|oledb__interface_bad_choice.condition|param_exact dbms oledb mode output param_exact oledb__interface bulkload version :2.14.52|3|15|P?|@{0|}}
{30001002|XXparameter|ole__interface|sameas mssql__interface sameas msjet__interface sameas msaccess__interface sameas oledb__interface|3|13|v||{0|}}
{30001002|XXparameter|ole__interface.condition|param_exact db_is_ole True mode output|3|15|P?|@{0|}}
{30001002|XXparameter|odbc__interface|api|15|1|Fl|Load data using api|{1|api|}}
{30001002|XXparameter|odbc__interface.condition|param_exact dbms odbc mode output|3|15|P?|@{0|}}
{30001002|XXparameter|greenplum__interface|copy|15|1|Fl|Load data using api or utility|{3|api|copy|gpfdist|}}
{30001002|XXparameter|greenplum__interface.condition|param_exact dbms greenplum mode output|3|15|P?|@{0|}}
{30001002|XXparameter|postgresql__interface|copy|15|1|Fl|Load data using api or utility|{2|api|copy|}}
{30001002|XXparameter|postgresql__interface.condition|param_exact dbms postgresql mode output|3|15|P?|@{0|}}
{30001002|XXparameter|psqlodbc__interface|api|15|1|Fl|For utility loads, manually set the DBMS type parameter on the Data pane to Postgresql, Greenplum or Redshift and press 'NO' when asked if you want to resolve a conflict.  Do NOT change the dbms tag in the dbc file itself.|{1|api|}}
{30001002|XXparameter|psqlodbc__interface.condition|param_exact dbms psqlodbc mode output|3|15|P?|@{0|}}
{30001002|XXparameter|redshift__interface|copy|15|1|Fl|Load data using api or utility|{3|api|copy|copy_via_ssh|}}
{30001002|XXparameter|redshift__interface.condition|param_exact dbms redshift mode output|3|15|P?|@{0|}}
{30001002|XXparameter|sybase__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|sybase__interface.condition|param_exact dbms sybase mode output|3|15|P?|@{0|}}
{30001002|XXparameter|sybaseiq__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|sybaseiq__interface.condition|param_exact dbms sybaseiq mode output|3|15|P?|@{0|}}
{30001002|XXparameter|informix__interface|api|15|1|Fl|Load data using api or utility|{5|api|dbload|dbaccess_xps|pload|dbaccess_serial|}}
{30001002|XXparameter|informix__interface.condition|param_exact dbms informix mode output|3|15|P?|@{0|}}
{30001002|XXparameter|adabas__interface|api|15|1|Fl|Load data using api|{1|api|}}
{30001002|XXparameter|adabas__interface.condition|param_exact dbms adabas mode output|3|15|P?|@{0|}}
{30001002|XXparameter|nza__interface_old|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|nza__interface_old.condition|param_exact dbms netezza mode output version :3.2.3.m99|3|15|P?|@{0|}}
{30001002|XXparameter|nza__interface|external_table|15|1|Fl|Load data using api, utility, or remote external table|{3|api|utility|external_table|}}
{30001002|XXparameter|nza__interface.condition|param_exact dbms netezza mode output version 3.2.4:|3|15|P?|@{0|}}
{30001002|XXparameter|nza__interface_impl|sameas nza__interface sameas nza__interface_old|3|13|v||{0|}}
{30001002|XXparameter|nza__interface_impl.condition|param_exact dbms netezza mode output|3|15|P?|@{0|}}
{30001002|XXparameter|db2_400__interface|api|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|db2_400__interface.condition|param_exact dbms db2_400 mode output|3|15|P?|@{0|}}
{30001002|XXparameter|odbc_400__interface|api|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|odbc_400__interface.condition|param_exact dbms odbc_400 mode output|3|15|P?|@{0|}}
{30001002|XXparameter|aster__interface|api|15|1|Fl|Load data using api or utility|{2|api|ncluster_loader|}}
{30001002|XXparameter|aster__interface.condition|param_exact dbms aster mode output|3|15|P?|@{0|}}
{30001002|XXparameter|snow__interface|api|15|1|Fl|Load data using api mode or files-in-a-stage|{2|api|staged_load|}}
{30001002|XXparameter|snow__interface.condition|param_exact dbms snowflake mode output|3|15|P?|@{0|}}
{30001002|XXparameter|hana__interface|api|15|1|Fl|Load data using api or utility|{1|api|}}
{30001002|XXparameter|hana__interface.condition|param_exact dbms hana mode output version 3.2.5.r24:|3|15|P?|@{0|}}
{30001002|XXparameter|hbase__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|hbase__interface.condition|param_exact dbms hbase mode output version 3.2.5.r24:|3|15|P?|@{0|}}
{30001002|XXparameter|cassandra__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|cassandra__interface.condition|param_exact dbms cassandra mode output version 3.3.2.r8:|3|15|P?|@{0|}}
{30001002|XXparameter|jdbc__interface|api|15|1|Fl|Load data using api or utility|{1|api|}}
{30001002|XXparameter|jdbc__interface.condition|param_exact dbms jdbc mode output version 3.3.2.r8:|3|15|P?|@{0|}}
{30001002|XXparameter|hive__interface|api|15|1|Fl|Load data using api|{1|api|}}
{30001002|XXparameter|hive__interface.condition|param_exact dbms hive mode output version 3.3.4:|3|15|P?|@{0|}}
{30001002|XXparameter|impala__interface|api|15|1|Fl|Load data using api|{1|api|}}
{30001002|XXparameter|impala__interface.condition|param_exact dbms impala mode output version 3.3.5.r19:|3|15|P?|@{0|}}
{30001002|XXparameter|bigquery__interface|utility|15|1|RFl|Load data using api, utility, or storage_load|{3|api|utility|storage_load|}}
{30001002|XXparameter|bigquery__interface.condition|param_exact dbms bigquery mode output version 4.1.0.r1:|3|15|P?|@{0|}}
{30001002|XXparameter|exasol__interface|utility|15|1|RFl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|exasol__interface.condition|param_exact dbms exasol mode output version 4.1.4.r20:|3|15|P?|@{0|}}
{30001002|XXparameter|_ab_param_must_exist|1|3|2|H$||{0|}}
{30001002|XXparameter|not_ims|value dbms ims False value dbms * True default constant True|13|13|v||{0|}}
{30001002|XXparameter|not_ims_or_adabas|value dbms ims False value dbms adabas False value dbms * True default constant True|13|13|v||{0|}}
{30001002|XXparameter|not_unusual_dbms|value dbms ims False value dbms adabas False value dbms mongo False value dbms hbase False value dbms * True default constant True|13|13|v||{0|}}
{30001002|XXparameter|not_iq|value dbms sybaseiq False value dbms * True default constant True|13|13|v||{0|}}
{30001002|XXparameter|not_hbase|value dbms hbase False value dbms * True default constant True|13|13|v||{0|}}
{30001002|XXparameter|not_cassandra|value dbms cassandra False value dbms * True default constant True|13|13|v||{0|}}
{30001002|XXparameter|not_bigquery|value dbms bigquery False value dbms * True default constant True|13|13|v||{0|}}
{30001002|XXparameter|is_iq_util|value not_iq True False value sybaseiq_interface api False default constant True|13|13|v||{0|}}
{30001002|XXparameter|not_ims_or_iqutil|value not_ims False False value is_iq_util True False default constant True|13|13|v||{0|}}
{30001002|XXparameter|supports_num_error|value interface_out api False value dbms netezza True value dbms oracle True value dbms db2eee True value dbms db2ee True value dbms db2 True value dbms db2uss True value dbms sybase True value dbms sybaseiq True value dbms db2_400 True value dbms odbc_400 True value informix__interface dbload True value informix__interface pload True value informix__interface dbaccess_xps True value mssql__interface utility True value dbms cassandra True value exasol__interface utility True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|supports_rows_per_commit|value interface_out api False value interface_out bulkload False value dbms oracle True value dbms mssql True value dbms sybase True value dbms sybaseiq True value informix__interface dbload True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|db2eee_or_db2ee|value dbms db2eee True value dbms db2ee True value dbms db2 True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|supports_native_options|value ora__interface utility True value ora__interface api False value sybase__interface utility True value sybase__interface api False value sybaseiq__interface utility True value sybaseiq__interface api False value nza__interface_impl utility True value nza__interface_impl external_table True value nza__interface_impl api False value ole__interface utility True value ole__interface bulkload False value ole__interface api False value db2ee__interface utility True value db2ee__interface api False value db2eee__interface utility True value db2eee__interface api False value db2__interface utility True value db2__interface api False value db2uss__interface_impl utility True value db2uss__interface_impl api False value db2uss_interface_impl api False value utility DSNTIIAUL False value utility UNLOAD True value nza_interface external_table True value nza_interface api False value cassandra__interface utility True value cassandra__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|supports_control_file|value ora__interface utility True value ora__interface api False value sybase__interface utility True value sybase__interface api False value sybaseiq__interface utility True value sybaseiq__interface api False value ole__interface utility True value ole__interface bulkload False value ole__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|old_ablocal_prerequisite|True|13|1|l||{0|}}
{30001002|XXparameter|old_ablocal_prerequisite.condition|mode input param_exact not_unusual_dbms True param_exact interface api|3|15|P?|@{0|}}
{30001002|XXparameter|supports_ablocal_expr|value nza_interface external_table True value utility DSNTIAUL True value utility UNLOAD False value old_ablocal_prerequisite True True default constant False|13|13|v||{0|}}
{30001002|XXparameter|supports_gen_ldr_datatypes|value db2eee_or_db2ee True True value dbms db2uss True default constant False|13|13|v||{0|}}
{30001002|XXparameter|ftype_pref_nondelimited|value field_type_preference delimited False value field_type_preference fixed True value field_type_preference variable True default constant False|13|13|v||{0|}}
{30001002|XXparameter|supports_batchsize|value ole__interface bulkload True value ole__interface utility False value ole__interface api False value bigquery__interface storage_load True default constant False|13|13|v||{0|}}
{30001002|XXparameter|no_partition_name|True|13|1|l||{0|}}
{30001002|XXparameter|no_partition_name.condition|param_exact dbms sybase version :3.1.1|3|15|P?|@{0|}}
{30001002|XXparameter|supports_partition_name|value ora__interface utility True value ora__interface api False value no_partition_name True False value sybase__interface utility True value sybase__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|supports_reject_threshold|value interface_out api True value mssql__interface bulkload True value bigquery__interface utility True value bigquery__interface storage_load True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|supports_package|value interface_out api True value mssql__interface bulkload True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|supports_logging|value interface_out api True value mssql__interface bulkload True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|supports_logfile|value ter_util True True value interface_out copy True value interface_out copy_via_ssh True value interface_out gpfdist True value interface_out staged_load True value interface_out ncluster_loader True value interface_in staged_unload True value interface_in copy True value interface_in staged True value interface_out staged True value interface_in unload True default constant False|13|13|v||{0|}}
{30001002|XXparameter|supports_api_load_transforms|value interface_out api True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|supports_api_load_transforms.condition|version 3.2.5.r24.0:|3|15|P?|@{0|}}
{30001002|XXparameter|ter_api_encrypt_data|True|13|1|l||{0|}}
{30001002|XXparameter|ter_api_encrypt_data.condition|param_exact dbms teradata version 3.0.2:|3|15|P?|@{0|}}
{30001002|XXparameter|api_encrypt_data_helper|sameas ter_api_encrypt_data default constant False|13|13|v||{0|}}
{30001002|XXparameter|supports_encrypt_data|value api_encrypt_data_helper True True value ter_util True True default constant False|13|13|v||{0|}}
{30001002|XXparameter|interface_in|sameas ora_interface sameas ter_interface sameas db2eee_interface sameas db2ee_interface sameas db2_interface sameas db2uss_interface_impl sameas db2_400_interface sameas ole_interface sameas odbc_interface sameas odbc_400_interface sameas snow_interface sameas sybase_interface sameas sybaseiq_interface sameas informix_interface sameas ims_interface_old sameas ims_interface sameas adabas_interface sameas nza_interface_impl sameas mysql_interface sameas postgresql_interface sameas psqlodbc_interface sameas greenplum_interface sameas redshift_interface sameas vertica_interface sameas aster_interface sameas mongo_interface sameas hana_interface sameas hbase_interface sameas cassandra_interface sameas jdbc_interface sameas hive_interface sameas impala_interface sameas bigquery_interface sameas exasol_interface|3|13|v||{0|}}
{30001002|XXparameter|interface_in.condition|mode input|3|15|P?|@{0|}}
{30001002|XXparameter|interface_out|sameas ora__interface sameas ter__interface sameas db2eee__interface sameas db2ee__interface sameas db2__interface sameas db2uss__interface_impl sameas db2_400__interface sameas ole__interface sameas odbc__interface sameas odbc_400__interface sameas sybase__interface sameas sybaseiq__interface sameas informix__interface sameas adabas__interface sameas nza__interface_impl sameas mysql__interface sameas psqlodbc__interface sameas postgresql__interface sameas greenplum__interface sameas redshift__interface sameas vertica__interface sameas aster__interface sameas snow__interface sameas hana__interface sameas hbase__interface sameas cassandra__interface sameas jdbc__interface sameas hive__interface sameas impala__interface sameas bigquery__interface sameas exasol__interface|3|13|v||{0|}}
{30001002|XXparameter|interface_out.condition|mode output|3|15|P?|@{0|}}
{30001002|XXparameter|interface|sameas interface_in sameas interface_out|3|13|Kv||{0|}}
{30001002|XXparameter|fixed_size_dml|False|13|1|RFOl|Generate fixed size DML when possible|{0|}}
{30001002|XXparameter|fixed_size_dml.condition|param_exact not_ims_or_adabas True version :2.12.2.m4|3|15|P?|@{0|}}
{30001002|XXparameter|field_type_preference|delimited|15|1|RFKl|Format of generated dml|{3|delimited|variable|fixed|}}
{30001002|XXparameter|field_type_preference.condition|param_exact not_unusual_dbms True version 2.12.2.m5:|3|15|P?|@{0|}}
{30001002|XXparameter|generate_loader_datatypes|False|13|1|FKl|Any non-delimited data types in generated DML will be compatible with the utility loader.|{0|}}
{30001002|XXparameter|generate_loader_datatypes.condition|param_exact supports_gen_ldr_datatypes True param_exact ftype_pref_nondelimited True version 2.14.64:|3|15|P?|@{0|}}
{30001002|XXparameter|column_delimiter||3|2|FK$|Default generated column delimiter|{0|}}
{30001002|XXparameter|column_delimiter.condition|param_exact not_unusual_dbms True|3|15|P?|@{0|}}
{30001002|XXparameter|encrypt_data|False|13|1|FKl|Encrypt data between database client and server (if possible)|{0|}}
{30001002|XXparameter|encrypt_data.condition|param_exact supports_encrypt_data True|3|15|P?|@{0|}}
{30001002|XXparameter|dml_with_default_values|none|15|1|RFKl|Generate dml with db default values (if possible)|{3|none|null_preferred|default_preferred|}}
{30001002|XXparameter|dml_with_default_values.condition|param_exact not_unusual_dbms True param_exact not_cassandra True version 3.2.3.r18.0:|3|15|P?|@{0|}}
{30001002|XXparameter|logfile||18|2|FK$|Name of file to store utility log information (optional)|{0|}}
{30001002|XXparameter|logfile.condition|param_exact supports_logfile True|3|15|P?|@{0|}}
{30001002|XXparameter|max_rows||1|2|FK$|Maximum rows to unload|{0|}}
{30001002|XXparameter|max_rows.condition|mode input param_exact not_ims_or_iqutil True|3|15|P?|@{0|}}
{30001002|XXparameter|ablocal_expr||3|2|FK$|DML expression for ABLOCAL() in select|{0|}}
{30001002|XXparameter|ablocal_expr.condition|param_exact supports_ablocal_expr True|3|15|P?|@{0|}}
{30001002|XXparameter|remove_delimiters|False|13|1|RFOl|Remove delimiters found in data|{0|}}
{30001002|XXparameter|remove_delimiters.condition|param_exact interface api mode input version 2.7: param_exact not_unusual_dbms True|3|15|P?|@{0|}}
{30001002|XXparameter|delimiter_replacement||3|2|FK$|Replacement for field delimiters found in data|{0|}}
{30001002|XXparameter|delimiter_replacement.condition|param_exact interface api mode input version 2.7: param_exact not_unusual_dbms True|3|15|P?|@{0|}}
{30001002|XXparameter|non_ansimode||13|1|RFKl|If true, process the select statement using Teradata semantics (like Fast Export/PT Export does); false means use Ansi semantics|{0|}}
{30001002|XXparameter|non_ansimode.condition|param_exact ter_interface api version 2.15.2:|3|15|P?|@{0|}}
{30001002|XXparameter|num_errors|50|1|2|FK$|Maximum number of errors allowed|{0|}}
{30001002|XXparameter|num_errors.condition|mode output param_exact supports_num_error True|3|15|P?|@{0|}}
{30001002|XXparameter|commitNumber||1|2|FK$|Number of records per commit checkpoint|{0|}}
{30001002|XXparameter|commitNumber.condition|mode output param_exact interface api param_exact not_hbase True param_exact not_bigquery True|3|15|P?|@{0|}}
{30001002|XXparameter|commitTable||24|3|FKc|Database table to store commit progress|{0|}}
{30001002|XXparameter|commitTable.condition|mode output param_exact interface api param_exact not_hbase True param_exact not_bigquery True|3|15|P?|@{0|}}
{30001002|XXparameter|jobid||25|3|FKc|For special purpose use only - see 'Help'|{0|}}
{30001002|XXparameter|jobid.condition|mode output param_exact interface api param_exact not_hbase True param_exact not_bigquery True|3|15|P?|@{0|}}
{30001002|XXparameter|ignoreDuplicates|False|13|1|Fl|if true, ignore unique constraint violations|{0|}}
{30001002|XXparameter|ignoreDuplicates.condition|mode output param_exact interface api param_exact not_hbase True param_exact not_cassandra True param_exact not_bigquery True version 2.15:|3|15|P?|@{0|}}
{30001002|XXparameter|no_actions_ok|value ignoreDuplicates True True value ignoreDuplicates False False default constant False|13|13|Kv||{0|}}
{30001002|XXparameter|no_actions_ok.condition|mode output param_exact interface api version 2.15:|3|15|P?|@{0|}}
{30001002|XXparameter|arrayInterfaceBufferSize||1|2|FK$|Size of batch when using array mode|{0|}}
{30001002|XXparameter|arrayInterfaceBufferSize.condition|mode output param_exact interface api param_exact not_bigquery True version 3.0.1:|3|15|P?|@{0|}}
{30001002|XXparameter|rows_per_commit||1|2|FK$|Rows per commit|{0|}}
{30001002|XXparameter|rows_per_commit.condition|mode output param_exact supports_rows_per_commit True|3|15|P?|@{0|}}
{30001002|XXparameter|batch_size|4096|1|2|FK$|Records processed in each batch|{0|}}
{30001002|XXparameter|batch_size.condition|mode output param_exact supports_batchsize True|3|15|P?|@{0|}}
{30001002|XXparameter|is_serial|False|13|1|FKl|Set to true to specify a serial load.|{0|}}
{30001002|XXparameter|is_serial.condition|mode output param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|file_type|delimited|15|1|FKl|Format of data to be loaded. If 'fixed', Output Table reformats the data to the loader's specifications.|{2|delimited|fixed|}}
{30001002|XXparameter|file_type.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True version 2.14.67:|3|15|P?|@{0|}}
{30001002|XXparameter|file_type_modifier||25|3|FKc|Enter desired file type modifiers.|{0|}}
{30001002|XXparameter|file_type_modifier.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|copy_target||25|3|FKc|Refer to DB2 UDB documentation for description|{0|}}
{30001002|XXparameter|copy_target.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|row_count|0|1|2|FK$|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|row_count.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|save_count|0|1|2|FK$|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|save_count.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|data_buffer_size|0|1|2|FK$|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|data_buffer_size.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|sort_buffer_size|0|1|2|FK$|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|sort_buffer_size.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|hold_quiesce|False|13|1|FKl|Set to True to leave the utility in quiesced exclusive state.|{0|}}
{30001002|XXparameter|hold_quiesce.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|cpu_parallelism|1|1|2|FK$|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|cpu_parallelism.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|disk_parallelism|0|1|2|FK$|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|disk_parallelism.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|recoverable|False|13|1|FKl|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|recoverable.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|indexing_mode|autoselect|15|1|FKl|Refer to DB2 UDB documentation for description.|{4|autoselect|rebuild|incremental|deferred|}}
{30001002|XXparameter|indexing_mode.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|access_level|allow_no_access|15|1|FKl|Refer to DB2 UDB documentation for description.|{2|allow_no_access|allow_read_access|}}
{30001002|XXparameter|access_level.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|use_tablespace||25|3|FKc|Refer to DB2 UDB documentation for description|{0|}}
{30001002|XXparameter|use_tablespace.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True version 2.14.101:|3|15|P?|@{0|}}
{30001002|XXparameter|lock_with_force|False|13|1|FKl|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|lock_with_force.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|check_pending|deferred|15|1|FKl|Refer to DB2 UDB documentation for description.|{2|immediate|deferred|}}
{30001002|XXparameter|check_pending.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|statsopt|none|15|1|FKl|Refer to DB2 UDB documentation for description.|{2|none|use_profile|}}
{30001002|XXparameter|statsopt.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|use_exception_table|False|13|1|FKl|Set to True to use exception table.|{0|}}
{30001002|XXparameter|use_exception_table.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|exception_table||25|3|FKc|Use only if you want to explicity specify the name for the exception table.|{0|}}
{30001002|XXparameter|exception_table.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|aster_dummy|a|3|2|$||{0|}}
{30001002|XXparameter|aster_util_in|value aster_interface api False value aster_interface * True default constant False|13|13|v||{0|}}
{30001002|XXparameter|aster_util_in.condition|param_exact dbms aster mode input|3|15|P?|@{0|}}
{30001002|XXparameter|aster_util_out|value aster__interface api False value aster__interface * True default constant False|13|13|v||{0|}}
{30001002|XXparameter|aster_util_out.condition|param_exact dbms aster mode output|3|15|P?|@{0|}}
{30001002|XXparameter|aster_util|value aster_util_in True True value aster_util_out True True value aster_dummy * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|aster_util.condition|param_exact dbms aster|3|15|P?|@{0|}}
{30001002|XXparameter|uses_legacy_inheritance|False|13|1|FKl|Set to True if table uses legacy parent/child inheritance |{0|}}
{30001002|XXparameter|uses_legacy_inheritance.condition|param_exact aster_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|allow_errors|False|13|1|FKl|If true, write errors to error table OR (if false) abort load on first error|{0|}}
{30001002|XXparameter|allow_errors.condition|param_exact aster_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|error_table||3|2|FK$|User-specified errtable name (optional)|{0|}}
{30001002|XXparameter|error_table.condition|param_exact allow_errors True|3|15|P?|@{0|}}
{30001002|XXparameter|label||3|2|FK$|Optional string to identify errors from this load in error table|{0|}}
{30001002|XXparameter|label.condition|param_exact allow_errors True|3|15|P?|@{0|}}
{30001002|XXparameter|like_postg|value dbms postgresql True value dbms greenplum True value dbms redshift True value dbms Redshift True default constant False|13|13|v||{0|}}
{30001002|XXparameter|db_type|sameas dbms|3|13|Kv|psqlodbc needs this param set|{0|}}
{30001002|XXparameter|db_type.condition|param_exact like_postg True|3|15|P?|@{0|}}
{30001002|XXparameter|gpfdist_log_prefix||18|2|FK$|Use this to override the log file path prefix in the gpdfdist configuration file (optional)|{0|}}
{30001002|XXparameter|gpfdist_log_prefix.condition|param_exact greenplum__interface gpfdist|3|15|P?|@{0|}}
{30001002|XXparameter|stage_name||3|2|FK$|optional name of stage.  If specified, MUST be in format defined for dbc file's default_stage parameter.|{0|}}
{30001002|XXparameter|stage_name.condition|param_exact dbms snowflake param_exact interface staged_*|3|15|P?|@{0|}}
{30001002|XXparameter|put_parallel_threads||1|2|FK$|Number of threads to use with PUT command|{0|}}
{30001002|XXparameter|put_parallel_threads.condition|param_exact dbms snowflake param_exact interface staged_load|3|15|P?|@{0|}}
{30001002|XXparameter|max_file_size|2000000|3|2|FK$|optional maximum size per unloaded file.|{0|}}
{30001002|XXparameter|max_file_size.condition|param_exact dbms snowflake param_exact interface staged_unload|3|15|P?|@{0|}}
{30001002|XXparameter|inf_dummy|a|3|2|$||{0|}}
{30001002|XXparameter|inf_util_in|value informix_interface api False value informix_interface * True default constant False|13|13|v||{0|}}
{30001002|XXparameter|inf_util_in.condition|param_exact dbms informix mode input|3|15|P?|@{0|}}
{30001002|XXparameter|inf_util_out|value informix__interface api False value informix__interface * True default constant False|13|13|v||{0|}}
{30001002|XXparameter|inf_util_out.condition|param_exact dbms informix mode output|3|15|P?|@{0|}}
{30001002|XXparameter|inf_util|value inf_util_in True True value inf_util_out True True value inf_dummy * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|inf_util.condition|param_exact dbms informix|3|15|P?|@{0|}}
{30001002|XXparameter|inf_use_rej|value informix__interface dbaccess_xps True value informix__interface dbload True value informix__interface pload True value inf_dummy * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|inf_use_rej.condition|param_exact inf_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|idelim|\||3|2|FK$|One-character delimiter for Informix utilities|{0|}}
{30001002|XXparameter|idelim.condition|param_exact inf_util True|3|15|P?|@{0|}}
{30001002|XXparameter|pdqpriority||3|2|FK$|DEFAULT, LOW, HIGH, ...|{0|}}
{30001002|XXparameter|pdqpriority.condition|param_exact inf_util True|3|15|P?|@{0|}}
{30001002|XXparameter|jobname||3|2|RFK$|Jobname for High-performance Load/Unload|{0|}}
{30001002|XXparameter|jobname.condition|param_exact interface pload|3|15|P?|@{0|}}
{30001002|XXparameter|pload_job_method|new|15|1|RFKl||{3|new|overwrite|reuse|}}
{30001002|XXparameter|pload_job_method.condition|param_exact interface pload|3|15|P?|@{0|}}
{30001002|XXparameter|isolation||3|2|FK$|'set isolation' choices: DIRTY READ, COMMITTED READ, ...|{0|}}
{30001002|XXparameter|isolation.condition|param_exact inf_util_in True|3|15|P?|@{0|}}
{30001002|XXparameter|ansi_isolation|default|15|1|FKl|'set transaction' isolation choices|{5|default|READ COMMITTED|REPEATABLE READ|SERIALIZABLE|READ UNCOMMITTED|}}
{30001002|XXparameter|ansi_isolation.condition|param_exact inf_util_in True|3|15|P?|@{0|}}
{30001002|XXparameter|force_raw|False|13|1|FKl|If true, temporarily alters table log mode to RAW|{0|}}
{30001002|XXparameter|force_raw.condition|param_exact inf_util_out True param_exact informix__interface dbaccess_xps|3|15|P?|@{0|}}
{30001002|XXparameter|rejectfile||3|2|RFK$|Rejectfile specification for Informix load utility|{0|}}
{30001002|XXparameter|rejectfile.condition|param_exact inf_use_rej True|3|15|P?|@{0|}}
{30001002|XXparameter|dummy1|a|3|2|$||{0|}}
{30001002|XXparameter|ter_util_in|value ter_interface api False value ter_interface * True default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_util_in.condition|param_exact dbms teradata mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ter_util_out|value ter__interface api False value ter__interface * True default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_util_out.condition|param_exact dbms teradata mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ter_util|value ter_util_in True True value ter_util_out True True value dummy1 * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_util.condition|param_exact dbms teradata|3|15|P?|@{0|}}
{30001002|XXparameter|ter_wb_in|value ter_interface WB_* True value ter_interface PT_* True value ter_interface * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_wb_in.condition|param_exact dbms teradata mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ter_wb_out|value ter__interface WB_* True value ter__interface PT_* True value ter__interface * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_wb_out.condition|param_exact dbms teradata mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ter_wb|value ter_wb_in True True value ter_wb_out True True value dummy1 * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_wb.condition|param_exact dbms teradata|3|15|P?|@{0|}}
{30001002|XXparameter|ter_is_fload|value ter__interface Fastload True value ter__interface WB_load True value ter__interface PT_load True value ter__interface PT_api_load True value ter__interface AB_Fastload True value ter__interface * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_is_fload.condition|param_exact dbms teradata mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ter_is_tpump|value ter__interface Tpump True value ter__interface WB_stream True value ter__interface PT_stream True value ter__interface PT_api_stream True value ter__interface * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_is_tpump.condition|param_exact dbms teradata mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ter_m_or_t|value ter__interface PT_inserter False value ter_is_fload False True value ter_is_fload True False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_m_or_t.condition|param_exact ter_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|ter_is_mload_only|value ter__interface Multiload True value ter__interface * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_is_mload_only.condition|param_exact dbms teradata mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ter_show_brief|value ter_m_or_t True True value ter_util_in True True value dummy1 * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_show_brief.condition|param_exact dbms teradata|3|15|P?|@{0|}}
{30001002|XXparameter|brief|True|13|1|FKl|Ask utility to generate brief log info if TRUE|{0|}}
{30001002|XXparameter|brief.condition|param_exact ter_show_brief True|3|15|P?|@{0|}}
{30001002|XXparameter|sleep|6|1|2|FK$|Minutes to wait before retrying logon|{0|}}
{30001002|XXparameter|sleep.condition|param_exact ter_util True|3|15|P?|@{0|}}
{30001002|XXparameter|tenacity|0|1|2|FK$|Maximum hours to attempt logon before utility gives up|{0|}}
{30001002|XXparameter|tenacity.condition|param_exact ter_util True|3|15|P?|@{0|}}
{30001002|XXparameter|sessions||3|2|FK$|Number of  sessions|{0|}}
{30001002|XXparameter|sessions.condition|param_exact ter_util True|3|15|P?|@{0|}}
{30001002|XXparameter|instances|1|1|2|FK$|Number of Parallel Transporter operator instances|{0|}}
{30001002|XXparameter|instances.condition|param_exact ter_wb True|3|15|P?|@{0|}}
{30001002|XXparameter|logtab_name||3|2|FK$|User-specified logtable name (optional)|{0|}}
{30001002|XXparameter|logtab_name.condition|param_exact ter_util True|3|15|P?|@{0|}}
{30001002|XXparameter|no_spool|False|13|1|FKl|If True, do not spool output if possible|{0|}}
{30001002|XXparameter|no_spool.condition|param_exact ter_util_in True version 3.0.1:|3|15|P?|@{0|}}
{30001002|XXparameter|use_load_isolation|False|13|1|FKl|Use Teradata load isolation capability when loading data|{0|}}
{30001002|XXparameter|use_load_isolation.condition|param_exact ter_is_tpump True|3|15|P?|@{0|}}
{30001002|XXparameter|tablewait|0|1|2|FK$|Number of hours to wait on other load tasks before utility gives up|{0|}}
{30001002|XXparameter|tablewait.condition|param_exact ter_is_mload_only True|3|15|P?|@{0|}}
{30001002|XXparameter|vartext|False|13|1|FKl|Send data to Teradata load utility in VARTEXT mode|{0|}}
{30001002|XXparameter|vartext.condition|param_exact ter_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|delim|\||3|2|FK$|delimiter for Teradata load utility|{0|}}
{30001002|XXparameter|delim.condition|param_exact vartext True|3|15|P?|@{0|}}
{30001002|XXparameter|np_axsmod_dir||26|2|RFK$|Name of directory for Named Pipe AXSMOD storage (required)|{0|}}
{30001002|XXparameter|np_axsmod_dir.condition|param_exact ter_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|checkpoint||1|2|FK$|Checkpoint rate to be used by Teradata utility|{0|}}
{30001002|XXparameter|checkpoint.condition|param_exact ter_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|worktab_name||3|2|FK$|User-specified worktable name (optional)|{0|}}
{30001002|XXparameter|worktab_name.condition|param_exact ter_util_out True param_exact ter_m_or_t True param_exact ter_is_tpump False|3|15|P?|@{0|}}
{30001002|XXparameter|errtab_name||3|2|FK$|User-specified errtable name (optional) [will have 1,2.. appended]|{0|}}
{30001002|XXparameter|errtab_name.condition|param_exact ter_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|macrodb||3|2|FK$|Database to use for Tpump macro creation and usage|{0|}}
{30001002|XXparameter|macrodb.condition|param_exact ter_is_tpump True|3|15|P?|@{0|}}
{30001002|XXparameter|errlimit||1|2|FK$|number errors allowed|{0|}}
{30001002|XXparameter|errlimit.condition|param_exact ter_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|errpercent||1|2|FK$|% errors allowed|{0|}}
{30001002|XXparameter|errpercent.condition|param_exact ter_m_or_t True|3|15|P?|@{0|}}
{30001002|XXparameter|ignore_dupl_rows|False|13|1|FKl|If true, ignore duplicate rows on insert|{0|}}
{30001002|XXparameter|ignore_dupl_rows.condition|param_exact ter_m_or_t True|3|15|P?|@{0|}}
{30001002|XXparameter|serialize|False|13|1|FKl|Guarantee serializability of operations on the same key if true|{0|}}
{30001002|XXparameter|serialize.condition|param_exact ter_is_tpump True|3|15|P?|@{0|}}
{30001002|XXparameter|pack|1|1|2|FK$|# of records to pack into a Tpump request or a TPT Update buffer|{0|}}
{30001002|XXparameter|pack.condition|param_exact ter_m_or_t True|3|15|P?|@{0|}}
{30001002|XXparameter|rate||1|2|FK$|# of statements per minute to send to Teradata database|{0|}}
{30001002|XXparameter|rate.condition|param_exact ter_is_tpump True|3|15|P?|@{0|}}
{30001002|XXparameter|trace_directory||26|2|FK$|directory for trace/debug info (optional)|{0|}}
{30001002|XXparameter|trace_directory.condition|param_exact ter_util True|3|15|P?|@{0|}}
{30001002|XXparameter|axsmod_tracelevel|None|3|2|FK$|for axsmod debugging|{0|}}
{30001002|XXparameter|axsmod_tracelevel.condition|param_exact ter_util True|3|15|P?|@{0|}}
{30001002|XXparameter|pt_tracelevel|None|3|2|FK$|for Parallel Transporter (was TWB) debugging|{0|}}
{30001002|XXparameter|pt_tracelevel.condition|param_exact ter_wb True|3|15|P?|@{0|}}
{30001002|XXparameter|ora_util_out|value ora__interface utility True value ora__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ora_util_out.condition|param_exact dbms oracle mode output|3|15|P?|@{0|}}
{30001002|XXparameter|oracle_max_kb||1|2|FK$|max kilobytes for each parallel unload select|{0|}}
{30001002|XXparameter|oracle_max_kb.condition|param_exact dbms oracle mode input version 2.10.12.m12:|3|15|P?|@{0|}}
{30001002|XXparameter|parallel_mode|rowid|15|1|RFKl|method of automatic parallelism|{3|rowid|subpartition|partition|}}
{30001002|XXparameter|parallel_mode.condition|param_exact dbms oracle mode input version 2.14:|3|15|P?|@{0|}}
{30001002|XXparameter|include_partitions||17|2|FKf$|Optional list of partitions to extract|{0|}}
{30001002|XXparameter|include_partitions.condition|param_exact dbms oracle mode input version 2.15.4.r30:|3|15|P?|@{0|}}
{30001002|XXparameter|flashback_queries|False|13|1|FKl|Employ Oracle flashback queries|{0|}}
{30001002|XXparameter|flashback_queries.condition|param_exact dbms oracle mode input version 3.2.6.r28:|3|15|P?|@{0|}}
{30001002|XXparameter|system_change_number||1|2|FK$|optional System Change Number|{0|}}
{30001002|XXparameter|system_change_number.condition|param_exact flashback_queries True version 3.2.6.r28:|3|15|P?|@{0|}}
{30001002|XXparameter|direct|True|13|1|RFOl|Use SQL*Loader Direct Mode|{0|}}
{30001002|XXparameter|direct.condition|param_exact ora_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|num_discards||1|2|FK$|# discards allowed|{0|}}
{30001002|XXparameter|num_discards.condition|param_exact ora_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|bindsize||1|2|FK$|SQL*Loader BINDSIZE setting|{0|}}
{30001002|XXparameter|bindsize.condition|param_exact ora_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|control_file||17|2|FKf$|Optional database load utility control file|{0|}}
{30001002|XXparameter|control_file.condition|param_exact supports_control_file True version 2.7:|3|15|P?|@{0|}}
{30001002|XXparameter|partition_name||3|2|FK$|Database partition name|{0|}}
{30001002|XXparameter|partition_name.condition|param_exact supports_partition_name True|3|15|P?|@{0|}}
{30001002|XXparameter|native_options||25|3|FKc|Optional commands passed directly to the database load utility.|{0|}}
{30001002|XXparameter|native_options.condition|param_exact supports_native_options True|3|15|P?|@{0|}}
{30001002|XXparameter|syb_util_out|value sybase__interface utility True value sybase__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|syb_util_out.condition|param_exact dbms sybase mode output|3|15|P?|@{0|}}
{30001002|XXparameter|sybaseiq_util_out|value sybaseiq__interface utility True value sybaseiq__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|sybaseiq_util_out.condition|param_exact dbms sybaseiq mode output|3|15|P?|@{0|}}
{30001002|XXparameter|row_delimited_by|if appropriate|15|1|FKl|Generate ROW DELIMITED BY clause in control file when appropriate.|{2|if appropriate|never|}}
{30001002|XXparameter|row_delimited_by.condition|param_exact sybaseiq_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|quoted_identifiers|always|15|1|FKl|Quote column names in generated control file.|{2|always|never|}}
{30001002|XXparameter|quoted_identifiers.condition|param_exact sybaseiq_util_out True version :3.0.3.r14|3|15|P?|@{0|}}
{30001002|XXparameter|supports_constraint|True|13|1|Kl||{0|}}
{30001002|XXparameter|supports_constraint.condition|param_exact sybaseiq_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|unique_constraint|True|13|1|FKl|If True, include UNIQUE as a load constraint to ignore.|{0|}}
{30001002|XXparameter|unique_constraint.condition|param_exact sybaseiq_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|null_constraint|True|13|1|FKl|If True, include NULL as a load constraint to ignore.|{0|}}
{30001002|XXparameter|null_constraint.condition|param_exact sybaseiq_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|foreign_key_constraint|False|13|1|FKl|If True, include FOREIGN KEY as a load constraint to ignore.|{0|}}
{30001002|XXparameter|foreign_key_constraint.condition|param_exact sybaseiq_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|data_value_constraint|False|13|1|FKl|If True, include DATA VALUE as a load constraint to ignore.|{0|}}
{30001002|XXparameter|data_value_constraint.condition|param_exact sybaseiq_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|ims_api_in|value ims_interface_old api True value ims_interface api True value ims_interface ims_connect False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ims_api_in.condition|param_exact dbms ims mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ims_remote_in|value ims_interface ims_connect True value ims_interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ims_remote_in.condition|param_exact dbms ims mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ims_api_or_remote_in|value ims_api_in True True value ims_remote_in True True default constant False|13|13|v||{0|}}
{30001002|XXparameter|batch_mode|DLI|15|1|RFKl|Run standalone batch or in a batch message processing region|{2|DLI|BMP|}}
{30001002|XXparameter|batch_mode.condition|param_exact ims_api_in True|3|15|P?|@{0|}}
{30001002|XXparameter|parent_segment_data|key_only|15|1|RFKl|Unload just the keys from parents, or all the data|{2|key_only|full_segment|}}
{30001002|XXparameter|parent_segment_data.condition|param_exact ims_api_or_remote_in True|3|15|P?|@{0|}}
{30001002|XXparameter|generate_unique_key|False|13|1|FKl|Generate a unique for segments with no unique key|{0|}}
{30001002|XXparameter|generate_unique_key.condition|param_exact ims_api_or_remote_in True|3|15|P?|@{0|}}
{30001002|XXparameter|_ab_ims_path_command|m_db ims_path|3|2|$|Command to generate list of segments|{0|}}
{30001002|XXparameter|_ab_ims_path_command_params|config_file positional table_spec keyword|3|2|$|List of parameters for path command|{0|}}
{30001002|XXparameter|ims_path|generated _ab_ims_path_command page_change|3|13|FHv|List of segments from root to segment|{0|}}
{30001002|XXparameter|ims_path.is_index|true|3|9|P|@{0|}}
{30001002|XXparameter|ims_path.condition|param_exact dbms ims mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ssa_||3|2|FK$|SSA qualifier for this segment|{0|}}
{30001002|XXparameter|ssa_.index|ims_path|3|9|P|@{0|}}
{30001002|XXparameter|ssa_.index_name|ssa_|3|9|P|@{0|}}
{30001002|XXparameter|ssa_.condition|param_exact dbms ims mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ims_parameters||3|2|FK$|Other ims parameters|{0|}}
{30001002|XXparameter|ims_parameters.condition|param_exact ims_api_in True|3|15|P?|@{0|}}
{30001002|XXparameter|alternate_psbname||3|2|FK$|Alternate PSB name to use when starting DFSRRC00|{0|}}
{30001002|XXparameter|alternate_psbname.condition|param_exact ims_api_in True|3|15|P?|@{0|}}
{30001002|XXparameter|ims_batch_size|10|1|2|RFK$|Number of records to request per call|{0|}}
{30001002|XXparameter|ims_batch_size.condition|param_exact ims_remote_in True|3|15|P?|@{0|}}
{30001002|XXparameter|file_number||1|2|FK$|Set to valid file number to bypass PREDICT for file number lookup.|{0|}}
{30001002|XXparameter|file_number.condition|param_exact dbms adabas|3|15|P?|@{0|}}
{30001002|XXparameter|backup_dataset||3|2|FK$|DSN of backup tape|{0|}}
{30001002|XXparameter|backup_dataset.condition|mode input param_exact dbms adabas param_exact adabas_interface backup_tape|3|15|P?|@{0|}}
{30001002|XXparameter|file_password||3|2|FK$|Adabas password in encrypted form (m_passwd).|{0|}}
{30001002|XXparameter|file_password.condition|param_exact dbms adabas|3|15|P?|@{0|}}
{30001002|XXparameter|nza_util_out|value nza__interface_impl utility True value nza__interface_impl external_table True value nza__interface_impl api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|nza_util_out.condition|param_exact dbms netezza mode output|3|15|P?|@{0|}}
{30001002|XXparameter|file_format|text|15|1|FKl|Format of data to be loaded. If 'fixed', Output Table reformats the data.|{2|text|fixed|}}
{30001002|XXparameter|file_format.condition|mode output param_exact nza__interface_impl external_table version 4.1.1.r9:|3|15|P?|@{0|}}
{30001002|XXparameter|local_log_path||3|2|FK$|local path to log directory mapped from NPS server|{0|}}
{30001002|XXparameter|local_log_path.condition|version :3.2.0|3|15|P?|@{0|}}
{30001002|XXparameter|server_log_path||3|2|FK$|path on NPS server to log directory|{0|}}
{30001002|XXparameter|server_log_path.condition|version :3.2.0|3|15|P?|@{0|}}
{30001002|XXparameter|db2uss_util_out|value db2uss__interface_impl utility True value db2uss__interface_impl api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|db2uss_util_out.condition|param_exact dbms db2uss mode output|3|15|P?|@{0|}}
{30001002|XXparameter|utility|DSNTIAUL|15|1|FKl|IBM utility used to unload the database|{2|DSNTIAUL|UNLOAD|}}
{30001002|XXparameter|utility.condition|mode input param_exact db2uss_interface_impl utility version 2.13.1:|3|15|P?|@{0|}}
{30001002|XXparameter|supports_utility_id|value db2uss_util_out True True value utility UNLOAD True value utility DSNTIAUL False default constant False|13|13|v||{0|}}
{30001002|XXparameter|utility_ID||3|2|FK$|Unique ID for DB2 utility job|{0|}}
{30001002|XXparameter|utility_ID.condition|param_exact interface utility param_exact supports_utility_id True version 2.13.1:|3|15|P?|@{0|}}
{30001002|XXparameter|temp_dataset||28|2|FK$|MVS dataset for UNLOAD output|{0|}}
{30001002|XXparameter|temp_dataset.condition|mode input param_exact db2uss_interface_impl utility param_exact utility UNLOAD version 2.13.1:|3|15|P?|@{0|}}
{30001002|XXparameter|image_copy_dataset||28|2|FK$|Optional DB/2 image copy dataset or datasets from which to unload data|{0|}}
{30001002|XXparameter|image_copy_dataset.condition|mode input param_exact db2uss_interface_impl utility param_exact utility UNLOAD version 2.14.68:|3|15|P?|@{0|}}
{30001002|XXparameter|tablespace||3|2|FK$|If unloading from an image copy dataset, the name of the tablespace which contains the data from the target table|{0|}}
{30001002|XXparameter|tablespace.condition|mode input param_exact db2uss_interface_impl utility param_exact utility UNLOAD version 2.14.68:|3|15|P?|@{0|}}
{30001002|XXparameter|partition||3|2|FK$|If unloading from a partitioned table, the partition number or range to unload|{0|}}
{30001002|XXparameter|partition.condition|mode input param_exact db2uss_interface_impl utility param_exact utility UNLOAD version 3.5.2.r9:|3|15|P?|@{0|}}
{30001002|XXparameter|SYSREC_dataset||28|2|FK$|MVS dataset for utility input|{0|}}
{30001002|XXparameter|SYSREC_dataset.condition|param_exact db2uss_util_out True version 2.13.1:|3|15|P?|@{0|}}
{30001002|XXparameter|SYSDISC_dataset||28|2|FK$|Optional MVS dataset for utility rejects|{0|}}
{30001002|XXparameter|SYSDISC_dataset.condition|param_exact db2uss_util_out True version 2.13.1:|3|15|P?|@{0|}}
{30001002|XXparameter|SYSERR_dataset||28|2|FK$|Optional MVS dataset for utility error output|{0|}}
{30001002|XXparameter|SYSERR_dataset.condition|param_exact db2uss_util_out True version 2.13.1:|3|15|P?|@{0|}}
{30001002|XXparameter|SYSUT1_dataset||28|2|FK$|Optional MVS work dataset for sort input|{0|}}
{30001002|XXparameter|SYSUT1_dataset.condition|param_exact db2uss_util_out True version 2.13.1:|3|15|P?|@{0|}}
{30001002|XXparameter|SORTOUT_dataset||28|2|FK$|Optional MVS work dataset for sort output|{0|}}
{30001002|XXparameter|SORTOUT_dataset.condition|param_exact db2uss_util_out True version 2.13.1:|3|15|P?|@{0|}}
{30001002|XXparameter|SYSMAP_dataset||28|2|FK$|Optional MVS work dataset for utility error processing|{0|}}
{30001002|XXparameter|SYSMAP_dataset.condition|param_exact db2uss_util_out True version 2.13.1:|3|15|P?|@{0|}}
{30001002|XXparameter|SYSCOPY_dataset||28|2|FK$|Optional MVS dataset for an inline image copy|{0|}}
{30001002|XXparameter|SYSCOPY_dataset.condition|param_exact db2uss_util_out True version 3.5.3.r11:|3|15|P?|@{0|}}
{30001002|XXparameter|use_mssql_bulk_out|value ole__interface bulkload True value ole__interface utility False value ole__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|staged_load_unload|value mssql__interface staged True value mssql__interface utility False value mssql__interface bulkload False value mssql__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|keep_identity|False|13|1|FKl|When "True" user must provide values for identity columns.|{0|}}
{30001002|XXparameter|keep_identity.condition|param_exact use_mssql_bulk_out True|3|15|P?|@{0|}}
{30001002|XXparameter|keep_nulls|False|13|1|FKl|When "true" a null input value will store a null, not the column default value.|{0|}}
{30001002|XXparameter|keep_nulls.condition|param_exact use_mssql_bulk_out True|3|15|P?|@{0|}}
{30001002|XXparameter|hints||3|2|FK$|The same as the "-h" option to bcp.|{0|}}
{30001002|XXparameter|hints.condition|param_exact use_mssql_bulk_out True|3|15|P?|@{0|}}
{30001002|XXparameter|storage_type|blob|15|1|FKl|Type of Azure data storage used for the staging area. blob - Azure Blob Storage; adlsg2 - Azure Data Lake Storage Gen2|{2|blob|adlsg2|}}
{30001002|XXparameter|storage_type.condition|param_exact staged_load_unload True|3|15|P?|@{0|}}
{30001002|XXparameter|as400_util_out|value db2_400__interface utility True value db2_400__interface api False value odbc_400__interface utility True value odbc_400__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|hostname||3|2|FK$|Name of AS/400 system|{0|}}
{30001002|XXparameter|hostname.condition|param_exact as400_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|rebuild_loader|False|13|1|FKl|If True, rebuild the RPG table loader program on the AS/400.|{0|}}
{30001002|XXparameter|rebuild_loader.condition|param_exact as400_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|ifs_data_path||3|2|FK$|Location, on the AS/400, where data will be landed|{0|}}
{30001002|XXparameter|ifs_data_path.condition|param_exact as400_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|ifs_dml_path||3|2|FK$|Location, on the AS/400, where dml files will be landed|{0|}}
{30001002|XXparameter|ifs_dml_path.condition|param_exact as400_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|mongo_api_in|value mongo_interface api True default constant False|13|13|v||{0|}}
{30001002|XXparameter|mongo_api_in.condition|param_exact dbms mongo mode input|3|15|P?|@{0|}}
{30001002|XXparameter|continue_on_bad_type|False|13|1|RFKl|False will abort if a Mongo document is read which cannot be translated into the port DML.  True will skip the document and continue processing.|{0|}}
{30001002|XXparameter|continue_on_bad_type.condition|param_exact mongo_api_in True version 3.2.3.r18:|3|15|P?|@{0|}}
{30001002|XXparameter|hbase_api|value interface_out utility False value dbms hbase True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|hbase_utility|value interface_out utility True value dbms hbase True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|filterstring||3|2|FK$|Thrift filterstring|{0|}}
{30001002|XXparameter|filterstring.condition|param_exact dbms hbase mode input|3|15|P?|@{0|}}
{30001002|XXparameter|columns||3|2|FK$|List of column names to load or unload; names are <family>:<qualifier> and separated by spaces|{0|}}
{30001002|XXparameter|columns.condition|param_exact dbms hbase param_exact hbase_api True|3|15|P?|@{0|}}
{30001002|XXparameter|timestamp||1|2|FK$|target timestamp (8-byte signed integer)|{0|}}
{30001002|XXparameter|timestamp.condition|param_exact dbms hbase|3|15|P?|@{0|}}
{30001002|XXparameter|start_rowkey||3|2|FK$|lower rowkey bound (inclusive) to unload|{0|}}
{30001002|XXparameter|start_rowkey.condition|param_exact dbms hbase mode input|3|15|P?|@{0|}}
{30001002|XXparameter|stop_rowkey||3|2|FK$|upper rowkey bound (exclusive) to unload|{0|}}
{30001002|XXparameter|stop_rowkey.condition|param_exact dbms hbase mode input|3|15|P?|@{0|}}
{30001002|XXparameter|skip_WAL|False|13|1|FKl|omit write to HBase WAL|{0|}}
{30001002|XXparameter|skip_WAL.condition|param_exact dbms hbase mode output param_exact hbase_api True|3|15|P?|@{0|}}
{30001002|XXparameter|sort_rowkeys|True|13|1|FKl|Sort incoming record stream on rowkey|{0|}}
{30001002|XXparameter|sort_rowkeys.condition|param_exact dbms hbase mode output|3|15|P?|@{0|}}
{30001002|XXparameter|sort_columns|True|13|1|FKl|Sort value vectors in each record on family and column names|{0|}}
{30001002|XXparameter|sort_columns.condition|param_exact dbms hbase mode output|3|15|P?|@{0|}}
{30001002|XXparameter|default_family||3|2|FK$|default column_family (used for flat record format only)|{0|}}
{30001002|XXparameter|default_family.condition|param_exact dbms hbase|3|15|P?|@{0|}}
{30001002|XXparameter|rowkey_field||3|2|FK$|field name for rowkey (used for flat record format only)|{0|}}
{30001002|XXparameter|rowkey_field.condition|param_exact dbms hbase|3|15|P?|@{0|}}
{30001002|XXparameter|tmp_file_mode|660|3|2|FK$|octal file mode must allow read by HBase server|{0|}}
{30001002|XXparameter|tmp_file_mode.condition|param_exact dbms hbase param_exact hbase_utility True param_exact hbase_api False|3|15|P?|@{0|}}
{30001002|XXparameter|hbase_configfile||3|2|FK$|optional configuration file used by HBase for load|{0|}}
{30001002|XXparameter|hbase_configfile.condition|param_exact dbms hbase param_exact hbase_utility True param_exact hbase_api False|3|15|P?|@{0|}}
{30001002|XXparameter|cassandra_api|value interface_out utility False value dbms cassandra True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|cassandra_utility|value interface_out utility True value dbms cassandra True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|chunksize||1|2|FK$|Chunk size for worker processes|{0|}}
{30001002|XXparameter|chunksize.condition|param_exact dbms cassandra mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ingestrate||1|2|FK$|Approx. ingest rate|{0|}}
{30001002|XXparameter|ingestrate.condition|param_exact dbms cassandra mode output|3|15|P?|@{0|}}
{30001002|XXparameter|max_batchsize||1|2|FK$|Max size of import batch|{0|}}
{30001002|XXparameter|max_batchsize.condition|param_exact dbms cassandra mode output|3|15|P?|@{0|}}
{30001002|XXparameter|min_batchsize||1|2|FK$|Min size of import batch|{0|}}
{30001002|XXparameter|min_batchsize.condition|param_exact dbms cassandra mode output|3|15|P?|@{0|}}
{30001002|XXparameter|num_processes||1|2|FK$|Number of worker processes|{0|}}
{30001002|XXparameter|num_processes.condition|param_exact dbms cassandra mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ttl_seconds||1|2|FK$|Time to live in seconds|{0|}}
{30001002|XXparameter|ttl_seconds.condition|param_exact dbms cassandra mode output|3|15|P?|@{0|}}
{30001002|XXparameter|bq_util_in|value bigquery_interface utility True value bigquery_interface * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|bq_util_in.condition|param_exact dbms bigquery mode input|3|15|P?|@{0|}}
{30001002|XXparameter|bq_util_out|value bigquery__interface utility True value bigquery__interface * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|bq_util_out.condition|param_exact dbms bigquery mode output|3|15|P?|@{0|}}
{30001002|XXparameter|bq_util|value bq_util_in True True value bq_util_out True True default constant False|13|13|v||{0|}}
{30001002|XXparameter|bq_storage_in|value bigquery_interface storage_extract True value bigquery_interface * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|bq_storage_in.condition|param_exact dbms bigquery mode input|3|15|P?|@{0|}}
{30001002|XXparameter|bq_storage_out|value bigquery__interface storage_load True value bigquery__interface * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|bq_storage_out.condition|param_exact dbms bigquery mode output|3|15|P?|@{0|}}
{30001002|XXparameter|bq_storage|value bq_storage_in True True value bq_storage_out True True default constant False|13|13|v||{0|}}
{30001002|XXparameter|schema_file||3|2|RFK$|Avro schema file representing table schema|{0|}}
{30001002|XXparameter|schema_file.condition|param_exact bq_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|ignore_unknown|False|13|1|RFKl|True allows the load to succeed despite the presence of columns not found in the table schema|{0|}}
{30001002|XXparameter|ignore_unknown.condition|param_exact bq_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|allow_schema_update|False|13|1|RFl|True allows the table schema to be updated as a side effect of the load|{0|}}
{30001002|XXparameter|allow_schema_update.condition|param_exact bq_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|allow_field_addition|False|13|1|FKl|True allows adding nullable fields to the table schema.|{0|}}
{30001002|XXparameter|allow_field_addition.condition|param_exact bq_util_out True param_exact allow_schema_update True|3|15|P?|@{0|}}
{30001002|XXparameter|allow_field_relaxation|False|13|1|FKl|True allows changing required fields to nullable in the table schema|{0|}}
{30001002|XXparameter|allow_field_relaxation.condition|param_exact bq_util_out True param_exact allow_schema_update True|3|15|P?|@{0|}}
{30001002|XXparameter|ss_escaped_by||3|2|FK$|[SQL statement modifier] A string (usually, one-character) defining a character escape sequence.  If non-empty, the SQL statement used for loading the data will be modified accordingly.  A given database's rules may be fairly complex -- read its documentation if you specify any value here.|{0|}}
{30001002|XXparameter|ss_escaped_by.condition|mode output param_exact supports_ss_escaped_by True|3|15|P?|@{0|}}
{30001002|XXparameter|supports_ss_escaped_by|value mysql__interface utility True value mysql__interface api False default constant False|13|13|Hv||{0|}}
{30001002|XXparameter|ss_string_for_null||3|2|FK$|[SQL statement modifier] The string representing the database NULL.  An empty string here means just that, not the database-specific default, which has to be specified explicitly.  Read the database documentation for a bulk-insert SQL statement to find the latter.|{0|}}
{30001002|XXparameter|ss_string_for_null.condition|mode output param_exact supports_ss_string_for_null True|3|15|P?|@{0|}}
{30001002|XXparameter|supports_ss_string_for_null|value vertica__interface utility True value vertica__interface api False default constant False|13|13|Hv||{0|}}
{30001002|XXparameter|ss_appendix||3|2|FK$|[SQL statement modifier] A free-form string going to the end of the SQL statement.  An arbitrary string to be appended to the end of the SQL statement used for data  loading (e.g. DIRECT).  Use this carefully to experiment with loading options:  there is no guarantee that the modified statement will work or result in a better  performance.  Read the database documentation and watch logs to understand the  possible modifications here and their operational impact.|{0|}}
{30001002|XXparameter|ss_appendix.condition|mode output param_exact supports_ss_appendix True|3|15|P?|@{0|}}
{30001002|XXparameter|supports_ss_appendix|value mysql__interface utility True value mysql__interface api False value postgresql__interface copy True value postgresql__interface api False value greenplum__interface api False value greenplum__interface copy True value greenplum__interface gpfdist True value vertica__interface utility True value vertica__interface api False default constant False|13|13|Hv||{0|}}
{30001002|XXparameter|mysql_interface|api|15|1|Fl|Unload data via api|{1|api|}}
{30001002|XXparameter|mysql_interface.condition|param_exact dbms mysql mode input|3|15|P?|@{0|}}
{30001002|XXparameter|mysql_util_in|value mysql_interface utility True value mysql_interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|mysql_util_in.condition|param_exact dbms mysql mode input|3|15|P?|@{0|}}
{30001002|XXparameter|mysql__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|mysql__interface.condition|param_exact dbms mysql mode output|3|15|P?|@{0|}}
{30001002|XXparameter|mysql_util_out|value mysql__interface utility True value mysql__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|mysql_util_out.condition|param_exact dbms mysql mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ss_duplicate_rows_handling|ignore|15|1|FKl|[SQL statement modifier] Handling of input rows that duplicate existing rows on unique key values.  With 'ignore', input rows that duplicate an existing row on a unique key value are skipped.  With 'replace', input rows replace existing rows.|{2|ignore|replace|}}
{30001002|XXparameter|ss_duplicate_rows_handling.condition|param_exact mysql_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|ss_character_set||3|2|FK$|[SQL statement modifier] Character set that MySQL uses to interpret the incoming data flow.  If not specified, determined by MySQL server 'character_set_database' system variable.|{0|}}
{30001002|XXparameter|ss_character_set.condition|param_exact mysql_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|vertica_interface|api|15|1|Fl|Unload data via api|{1|api|}}
{30001002|XXparameter|vertica_interface.condition|param_exact dbms vertica mode input|3|15|P?|@{0|}}
{30001002|XXparameter|vertica_util_in|value vertica_interface utility True value vertica_interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|vertica_util_in.condition|param_exact dbms vertica mode input|3|15|P?|@{0|}}
{30001002|XXparameter|vertica__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|vertica__interface.condition|param_exact dbms vertica mode output|3|15|P?|@{0|}}
{30001002|XXparameter|vertica_util_out|value vertica__interface utility True value vertica__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|vertica_util_out.condition|param_exact dbms vertica mode output|3|15|P?|@{0|}}
{30001002|XXparameter|gendml_param_names|column_delimiter fixed_size_dml field_type_preference dml_with_default_values generate_loader_datatypes ablocal_expr parent_segment_data generate_unique_key file_password file_number|3|2|H$||{0|}}
{30001002|XXparameter|m_db_layout_params|ablocal_expr|3|2|H$||{0|}}
{30001002|XXparameter|m_db_layout_params_db2uss|ablocal_expr db2uss_util_out utility|3|2|H$||{0|}}
{30001002|XXparameter|m_db_layout_params_db2uss.condition|param_exact dbms db2uss|3|15|P?|@{0|}}
{30001002|XXparameter|m_db_layout_param_names|sameas m_db_layout_params_db2uss sameas m_db_layout_params|3|13|Hv||{0|}}
{30001002|XXparameter|m_db_dump_param_names|remove_delimiters delimiter_replacement ablocal_expr|3|2|H$||{0|}}
{30001002|XXparameter|dbms||3|2|RF$|Database|{0|}}
{30001002|XXparameter|override_mpname|value dbms ims itable-ims value dbms mongo itable-mongo default dollar_substitution itable|3|13|v||{0|}}
{30001002|XXparameter|override_mpname.display_name|override-mpname|3|9|P|@{0|}}
{30001002|XXparameter|override_mpname.condition|mode input mode input|3|15|P?|@{0|}}
{30001002|XXparameter|package||8|1|FKl|Functions for deciding whether to abort or retry a transaction|{0|}}
{30001002|XXparameter|package.condition|mode output param_exact supports_package True version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|reject_threshold|Abort on first reject|15|1|Fl|When to abort if input records are rejected|{3|Abort on first reject|Never abort|Use limit/ramp|}}
{30001002|XXparameter|reject_threshold.display_name|reject-threshold|3|9|P|@{0|}}
{30001002|XXparameter|reject_threshold.condition|mode output param_exact supports_reject_threshold True version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|limit|0|1|2|F$|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|limit.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|ramp|0.0|2|2|F$|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|limit_keyword|value reject-threshold Never* 0 value reject-threshold Abort* 0 sameas limit default dollar_substitution 0|1|13|Kv|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|ramp_keyword|value reject-threshold Never* -1 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword.condition|version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|logging|False|13|1|Fl|Log internal events|{0|}}
{30001002|XXparameter|logging.condition|mode output param_exact supports_logging True version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|log|log_concat|3|13|Kv|Special log parameter|{0|}}
{30001002|XXparameter|log.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_input||15|1|Fl|Frequency of input records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_input.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_reject||15|1|Fl|Frequency of reject records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_reject.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|supports_error_group|value interface api True value mssql__interface bulkload True value ter_util True True value dbms * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|error_group||3|2|F$|Optional group name identifying a Handle Errors component to which errors can be directed.|{0|}}
{30001002|XXparameter|error_group.condition|version 2.16: mode output param_exact supports_error_group True|3|15|P?|@{0|}}
{30001002|XXparameter|log_group||3|2|F$|Optional group name of a Handle Logs component to which log output can be directed.|{0|}}
{30001002|XXparameter|log_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|keyword_map|limit_keyword limit ramp_keyword ramp|3|2|$||{0|}}
{30001002|XXparameter|keyword_map.display_name|keyword-map|3|9|P|@{0|}}
{30001002|XXparameter|_ab_insert_statement_supported|value dbms ims False value dbms adabas False value interface_out api True value dbms teradata True value dbms informix True value dbms aster True value dbms snowflake True value dbms mongo False value dbms hbase False value dbms * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|_ab_select_statement_supported|value dbms "" True value dbms ims False value dbms adabas False value dbms mongo False value dbms hbase False value dbms * True default constant True|13|13|v||{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|_use_rich_log_format|True|13|1|Hl|False means use legacy native charset format|{0|}}
{30001002|XXparameter|Layout|$[[record kind 84 subkind 68 parts [vector]]]|9|9|RF||{0|}}
{30001002|XXparameter|eme_dataset_mapping||40|9|F|Place in the EME to create the dataset(s) corresponding to this component.|{0|}}
{30001002|XXparameter|read_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|log_metadata|$AB_HOME/include/log-info.dml|7|2|Ff$||{0|}}
{30001002|XXparameter|mpcmodtime|1669401743|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagation|metadata type: log = remote("$AB_HOME/include/log-info.dml")|3|1|HKl|@{0|}}
{30001002|XXparameter|table_spec.custom_editor_key|SelectEditor|3|9|P|@{0|}}
}}@0|Input Table|0|0|0|0|0|0|0|@||1|100|-1|@6|@1|}}
{2010203004|XXGoport|53|0|124|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|read|0.0|@@@2448|0|}}
{2010203004|XXGoport|54|0|127|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010501005|XXGpvertex|55|0|136|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Reformat.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|output_indexes||3|2|f$|@{0|}}
{30001002|XXparameter|_propagate_through|metadata type: out = in
metadata type: in = out|3|1|l|@{0|}}
{30001002|XXparameter|transform0|$AI_XFR/GEN_CSV_FIRST_DEFINED.xfr|3|2|f$|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|out0_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject0_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
}}@0|Reformat|390000|120000|0|0|0|0|135|FD_RFMT-2|Ab Initio Software|Built-in 1.0:|1|100|0||6||36865|1|{1|0|}}}
{2010203004|XXGoport|56|0|138|0|{@{}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|57|0|141|0|{@{}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|58|0|143|0|{@{}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|59|0|145|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|60|0|147|0|{@{}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|61|0|150|0|{Changes the record format of your data by dropping fields or by using DML expressions to add fields, combine fields, or modify the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|reformat-transform|3|1|Hl||{0|}}
{30001002|XXparameter|image__|~ab_home/bin/unitool|3|2|H$||{0|}}
{30001002|XXparameter|count|1|1|1|Fl|Number of reformat transforms|{0|}}
{30001002|XXparameter|count.is_index|true|3|9|P|@{0|}}
{30001002|XXparameter|transform0||8|1|FOl|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
{30001002|XXparameter|select||20|2|FK$|Filter expression before reformatting|{0|}}
{30001002|XXparameter|error_group||3|2|F$|Optional group name identifying a Handle Errors component to which errors can be directed.|{0|}}
{30001002|XXparameter|error_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|log_group||3|2|F$|Optional group name of a Handle Logs component to which log output can be directed.|{0|}}
{30001002|XXparameter|log_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|reject_threshold|Abort on first reject|15|1|Fl|When to abort if input records are rejected|{3|Abort on first reject|Never abort|Use limit/ramp|}}
{30001002|XXparameter|reject_threshold.display_name|reject-threshold|3|9|P|@{0|}}
{30001002|XXparameter|limit|0|1|2|F$|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|limit.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|ramp|0.0|2|2|F$|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|limit_keyword|value reject-threshold Never* 0 value reject-threshold Abort* 0 sameas limit default dollar_substitution 0|1|13|Kv|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|ramp_keyword|value reject-threshold Never* 99.0 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword.condition|version :2.12.999.m999|3|15|P?|@{0|}}
{30001002|XXparameter|ramp_keyword2|value reject-threshold Never* -1 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword2.condition|version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|keyword_map|limit_keyword limit ramp_keyword ramp ramp_keyword2 ramp|3|2|$||{0|}}
{30001002|XXparameter|keyword_map.display_name|keyword-map|3|9|P|@{0|}}
{30001002|XXparameter|output_index||8|1|FKl|Output index computation|{0|}}
{30001002|XXparameter|output_index.display_name|output-index|3|9|P|@{0|}}
{30001002|XXparameter|output_index.keyword|output-index|3|9|P|@{0|}}
{30001002|XXparameter|output_index_port|discriminator|3|2|FHK$|Output index port for debugger|{0|}}
{30001002|XXparameter|output_index_port.display_name|output-index_port|3|9|P|@{0|}}
{30001002|XXparameter|output_index_port.keyword|output-index_port|3|9|P|@{0|}}
{30001002|XXparameter|output_index_signature|int output_index_out::output_index(in)|3|2|H$|Function returning index of output port|{0|}}
{30001002|XXparameter|output_index_signature.display_name|output-index_signature|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes||8|1|FKl|Output index vector computation|{0|}}
{30001002|XXparameter|output_indexes.display_name|output-indexes|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes.keyword|output-indexes|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_port|discriminator|3|2|FHK$|Output index port for debugger|{0|}}
{30001002|XXparameter|output_indexes_port.display_name|output-indexes_port|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_port.keyword|output-indexes_port|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_signature|int[int] output_indexes_out::output_indexes(in)|3|2|H$|Function returning vector of indexes of output ports|{0|}}
{30001002|XXparameter|output_indexes_signature.display_name|output-indexes_signature|3|9|P|@{0|}}
{30001002|XXparameter|logging|False|13|1|Fl|Log internal events|{0|}}
{30001002|XXparameter|log|log_concat|3|13|Kv|Special log parameter|{0|}}
{30001002|XXparameter|log.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_input||15|1|Fl|Frequency of input records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_input.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_output||15|1|Fl|Frequency of output records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_output.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_reject||15|1|Fl|Frequency of reject records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_reject.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|num_operations|7|1|2|H$|Number of operations in package|{0|}}
{30001002|XXparameter|operation1|out::reformat(in)|3|2|H$|Reformat operation|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out*|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|legacy_xfr;reject*=in;out*=in|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis||3|2|H$||{0|}}
{30001002|XXparameter|filter_aggregate_analysis|out=filter(select)|3|2|H$||{0|}}
{30001002|XXparameter|num_types|3|1|2|H$|Number of private data types in package|{0|}}
{30001002|XXparameter|type1|error_info_t error_info "record string('', charset='x-ab-internal') component; int port_index; string('', charset='x-ab-internal') parameter; string('', charset='x-ab-internal') message; record string('', charset='x-ab-internal') code; int parent_index; record string('', charset='x-ab-internal') name; string('', charset='x-ab-internal') value; end[int] attributes; end[int] details; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|type2|log_event_t log_event "record string('', charset='x-ab-internal') event_type; string('', charset='x-ab-internal') event_text; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|type3|event_info_t event_info "record int event_type; end"|3|2|H$|Information about the kind of event that happened|{0|}}
{30001002|XXparameter|operation2|optional out::output_for_error(error_info, in)|3|2|H$|Function to create output in case of error.|{0|}}
{30001002|XXparameter|operation3|optional error::make_error(error_info, in)|3|2|H$|Function to create error record in case of error.|{0|}}
{30001002|XXparameter|operation4|optional log_event::log_error(error_info, in)|3|2|H$|Function to create log record in case of error.|{0|}}
{30001002|XXparameter|operation5|optional log_event::final_log_output()|3|2|H$|Function to create final log record.|{0|}}
{30001002|XXparameter|operation6|optional out::output_at_event(event_info)|3|2|H$|Function to allow the output of an additional record at event|{0|}}
{30001002|XXparameter|operation7|optional ::on_startup(long in_recovery)|3|2|H$|Function executed at graph startup|{0|}}
{30001002|XXparameter|num_callbacks|6|1|2|H$|Number of callbacks in package|{0|}}
{30001002|XXparameter|_use_rich_error_format|True|13|1|Hl|False means use legacy string newline format|{0|}}
{30001002|XXparameter|_use_rich_log_format|True|13|1|Hl|False means use legacy native charset format|{0|}}
{30001002|XXparameter|Layout|@9|9|RFs||{0|}}
{30001002|XXparameter|main_mp_port|count out|3|1|l||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out0_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|reject0_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|error0_metadata|$AB_HOME/include/error-info.dml|7|2|Ff$||{0|}}
{30001002|XXparameter|log_metadata|$AB_HOME/include/log-info.dml|7|2|Ff$||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out[*],reject[*],error[*],log::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1669401747|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagation|metadata type: error = remote("$AB_HOME/include/error-info.dml")
metadata type: reject = in
metadata type: log = remote("$AB_HOME/include/log-info.dml")|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|Reformat|0|0|0|0|0|0|0|@||1|100|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|62|0|152|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
{30001002|XXparameter|transform0||8|8|FO=|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
}}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|63|0|156|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|64|0|159|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|65|0|162|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|66|0|165|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010505005|XXGtvertex|67|0|172|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Output_Table.mdc|3|2|Pf$|@{0|}}
{30001002|XXparameter|reject_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|config_file|$BQ_PUB_DB/bigquery_impersonate.dbc|3|2|f$|@{0|}}
{30001002|XXparameter|dbms|bigquery|3|2|$|@{0|}}
{30001002|XXparameter|table_spec|$\{IODS_PUB_BQ_DATASET_STG\}.STG_CONS_CSV_DENTAL_CLM_DTL_HX|3|3|c|@{0|}}
{30001002|XXparameter|eme_dataset_mapping|$[[record map_component 0 datasets NULL]]|3|9||@{0|}}
{30001002|XXparameter|mode|0x000A|3|2|$|@{0|}}
{30001002|XXparameter|bigquery__interface|storage_load|3|1|l|@{0|}}
{30001002|XXparameter|field_type_preference|variable|3|1|l|@{0|}}
{30001002|XXparameter|write_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
}}@0|Output Table|3380000|265000|0|0|0|0|125|Output Table|Ab Initio Software|Built-in 2.2:|1|100|0||6||1|}}
{2010203004|XXGoport|68|0|174|0|{@{}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|69|0|176|0|{@{}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|70|0|178|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|71|0|180|0|{@{}@0|0|0|0|write|0.0|@@@1936|0|}}
{2010505005|XXGtvertex|72|0|183|0|{Loads data records into a database.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mode|0x0012|1|2|FH$|modes of access|{0|}}
{30001002|XXparameter|HelpID|HICOMP_OUTPUT_TABLE|3|2|H$||{0|}}
{30001002|XXparameter|config_file||22|2|RFf$||{0|}}
{30001002|XXparameter|table_spec||32|3|RFc||{0|}}
{30001002|XXparameter|table_spec_kind|table|15|1|RFl||{3|table|insert|select|}}
{30001002|XXparameter|db_is_ole|value dbms mssql True value dbms msjet True value dbms msaccess True value dbms oledb True default constant False|13|13|v||{0|}}
{30001002|XXparameter|ora_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|ora_interface.condition|param_exact dbms oracle mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ter_interface_new|FastExport|15|1|Fl|Unload data via api or utility|{5|api|FastExport|PT_export|PT_selector|PT_api_export|}}
{30001002|XXparameter|ter_interface_new.condition|param_exact dbms teradata mode input version 2.12:|3|15|P?|@{0|}}
{30001002|XXparameter|ter_interface|sameas ter_interface_new|3|13|v||{0|}}
{30001002|XXparameter|ter_interface.condition|param_exact dbms teradata mode input|3|15|P?|@{0|}}
{30001002|XXparameter|db2eee_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|db2eee_interface.condition|param_exact dbms db2eee mode input|3|15|P?|@{0|}}
{30001002|XXparameter|db2ee_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|db2ee_interface.condition|param_exact dbms db2ee mode input|3|15|P?|@{0|}}
{30001002|XXparameter|db2_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|db2_interface.condition|param_exact dbms db2 mode input version 3.2.4.r21:|3|15|P?|@{0|}}
{30001002|XXparameter|db2uss_interface_old|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|db2uss_interface_old.condition|param_exact dbms db2uss mode input version :2.12.2.m99|3|15|P?|@{0|}}
{30001002|XXparameter|db2uss_interface|api|15|1|Fl|Unload data via api or utility|{2|api|utility|}}
{30001002|XXparameter|db2uss_interface.condition|param_exact dbms db2uss mode input version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|db2uss_interface_impl|sameas db2uss_interface sameas db2uss_interface_old|3|13|v||{0|}}
{30001002|XXparameter|db2uss_interface_impl.condition|param_exact dbms db2uss mode input|3|15|P?|@{0|}}
{30001002|XXparameter|mssql_interface|api|15|1|Fl|Unload data via api or staged|{2|api|staged|}}
{30001002|XXparameter|mssql_interface.condition|param_exact dbms mssql mode input|3|15|P?|@{0|}}
{30001002|XXparameter|msjet_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|msjet_interface.condition|param_exact dbms msjet mode input|3|15|P?|@{0|}}
{30001002|XXparameter|msaccess_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|msaccess_interface.condition|param_exact dbms msaccess mode input|3|15|P?|@{0|}}
{30001002|XXparameter|oledb_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|oledb_interface.condition|param_exact dbms oledb mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ole_interface|sameas mssql_interface sameas msjet_interface sameas msaccess_interface sameas oledb_interface|3|13|v||{0|}}
{30001002|XXparameter|ole_interface.condition|param_exact db_is_ole True mode input|3|15|P?|@{0|}}
{30001002|XXparameter|odbc_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|odbc_interface.condition|param_exact dbms odbc mode input|3|15|P?|@{0|}}
{30001002|XXparameter|greenplum_interface|api|15|1|Fl|Unload data via api|{1|api|}}
{30001002|XXparameter|greenplum_interface.condition|param_exact dbms greenplum mode input|3|15|P?|@{0|}}
{30001002|XXparameter|postgresql_interface|api|15|1|Fl|Unload data via api|{2|api|copy|}}
{30001002|XXparameter|postgresql_interface.condition|param_exact dbms postgresql mode input|3|15|P?|@{0|}}
{30001002|XXparameter|psqlodbc_interface|api|15|1|Fl|Unload data via api|{1|api|}}
{30001002|XXparameter|psqlodbc_interface.condition|param_exact dbms psqlodbc mode input|3|15|P?|@{0|}}
{30001002|XXparameter|redshift_interface|api|15|1|Fl|Unload data via api or utility|{2|api|unload|}}
{30001002|XXparameter|redshift_interface.condition|param_exact dbms redshift mode input|3|15|P?|@{0|}}
{30001002|XXparameter|snow_interface|api|15|1|Fl|Unload data via api or utility|{2|api|staged_unload|}}
{30001002|XXparameter|snow_interface.condition|param_exact dbms snowflake mode input|3|15|P?|@{0|}}
{30001002|XXparameter|sybase_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|sybase_interface.condition|param_exact dbms sybase mode input|3|15|P?|@{0|}}
{30001002|XXparameter|sybaseiq_interface|api|15|1|Fl|Unload data via api or utility|{2|api|utility|}}
{30001002|XXparameter|sybaseiq_interface.condition|param_exact dbms sybaseiq mode input|3|15|P?|@{0|}}
{30001002|XXparameter|informix_interface|api|15|1|Fl|Unload data via api or utility|{4|api|dbaccess_serial|dbaccess_xps|pload|}}
{30001002|XXparameter|informix_interface.condition|param_exact dbms informix mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ims_interface_old|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|ims_interface_old.condition|param_exact dbms ims mode input version :3.1.6.r26|3|15|P?|@{0|}}
{30001002|XXparameter|ims_interface|api|15|1|Fl|Unload data via api (local) or via IMS connect(remote) |{2|api|ims_connect|}}
{30001002|XXparameter|ims_interface.condition|param_exact dbms ims mode input version 3.1.6.r27:|3|15|P?|@{0|}}
{30001002|XXparameter|adabas_interface|api|15|1|Fl|Unload data via api or from backup tape|{2|api|backup_tape|}}
{30001002|XXparameter|adabas_interface.condition|param_exact dbms adabas mode input|3|15|P?|@{0|}}
{30001002|XXparameter|mongo_interface|api|15|1|Fl|Unload data via api |{1|api|}}
{30001002|XXparameter|mongo_interface.condition|param_exact dbms mongo mode input version 3.2.3.r18.0:|3|15|P?|@{0|}}
{30001002|XXparameter|hbase_interface|api|15|1|Fl|Unload data via api |{1|api|}}
{30001002|XXparameter|hbase_interface.condition|param_exact dbms hbase mode input version 3.2.5.r24:|3|15|P?|@{0|}}
{30001002|XXparameter|cassandra_interface|api|15|1|Fl|Unload data via api |{1|api|}}
{30001002|XXparameter|cassandra_interface.condition|param_exact dbms cassandra mode input version 3.3.2.r8:|3|15|P?|@{0|}}
{30001002|XXparameter|nza_interface_old|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|nza_interface_old.condition|param_exact dbms netezza mode input version :2.15.7.m99|3|15|P?|@{0|}}
{30001002|XXparameter|nza_interface|api|15|1|Fl|Unload data via api or remote external table|{2|api|external_table|}}
{30001002|XXparameter|nza_interface.condition|param_exact dbms netezza mode input version 2.15.8:|3|15|P?|@{0|}}
{30001002|XXparameter|nza_interface_impl|sameas nza_interface sameas nza_interface_old|3|13|v||{0|}}
{30001002|XXparameter|nza_interface_impl.condition|param_exact dbms netezza mode input|3|15|P?|@{0|}}
{30001002|XXparameter|db2_400_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|db2_400_interface.condition|param_exact dbms db2_400 mode input|3|15|P?|@{0|}}
{30001002|XXparameter|odbc_400_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|odbc_400_interface.condition|param_exact dbms odbc_400 mode input|3|15|P?|@{0|}}
{30001002|XXparameter|aster_interface|api|15|1|Fl|Unload data via api or utility|{2|api|ncluster_export|}}
{30001002|XXparameter|aster_interface.condition|param_exact dbms aster mode input|3|15|P?|@{0|}}
{30001002|XXparameter|hana_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|hana_interface.condition|param_exact dbms hana mode input version 3.2.5.r24:|3|15|P?|@{0|}}
{30001002|XXparameter|jdbc_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|jdbc_interface.condition|param_exact dbms jdbc mode input version 3.3.2.r8:|3|15|P?|@{0|}}
{30001002|XXparameter|hive_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|hive_interface.condition|param_exact dbms hive mode input version 3.3.4:|3|15|P?|@{0|}}
{30001002|XXparameter|impala_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|impala_interface.condition|param_exact dbms impala mode input version 3.3.5.r19:|3|15|P?|@{0|}}
{30001002|XXparameter|bigquery_interface|utility|15|1|RFl|Unload data via api, utility, or storage_extract|{3|api|utility|storage_extract|}}
{30001002|XXparameter|bigquery_interface.condition|param_exact dbms bigquery mode input version 4.1.1.r9:|3|15|P?|@{0|}}
{30001002|XXparameter|exasol_interface|utility|15|1|RFl|Unload data via api or utility|{2|api|utility|}}
{30001002|XXparameter|exasol_interface.condition|param_exact dbms exasol mode input version 4.1.4.r20:|3|15|P?|@{0|}}
{30001002|XXparameter|ora__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|ora__interface.condition|param_exact dbms oracle mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ter__interface|Multiload|15|1|Fl|Load data using api or utility|{11|api|Fastload|Multiload|Tpump|PT_load|PT_update|PT_stream|PT_inserter|PT_api_load|PT_api_update|PT_api_stream|}}
{30001002|XXparameter|ter__interface.condition|param_exact dbms teradata mode output|3|15|P?|@{0|}}
{30001002|XXparameter|db2eee__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|db2eee__interface.condition|param_exact dbms db2eee mode output|3|15|P?|@{0|}}
{30001002|XXparameter|db2ee__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|db2ee__interface.condition|param_exact dbms db2ee mode output|3|15|P?|@{0|}}
{30001002|XXparameter|db2__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|db2__interface.condition|param_exact dbms db2 mode output version 3.2.4.r21:|3|15|P?|@{0|}}
{30001002|XXparameter|db2uss__interface_old|api|15|1|Fl|Load data via api or utility|{1|api|}}
{30001002|XXparameter|db2uss__interface_old.condition|param_exact dbms db2uss mode output version :2.12.2.m99|3|15|P?|@{0|}}
{30001002|XXparameter|db2uss__interface|api|15|1|Fl|Load data via api or utility|{2|api|utility|}}
{30001002|XXparameter|db2uss__interface.condition|param_exact dbms db2uss mode output version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|db2uss__interface_impl|sameas db2uss__interface sameas db2uss__interface_old|3|13|v||{0|}}
{30001002|XXparameter|db2uss__interface_impl.condition|param_exact dbms db2uss mode output|3|15|P?|@{0|}}
{30001002|XXparameter|mssql__interface|utility|15|1|Fl|Load data using api, utility, bulkload or staged|{4|api|utility|bulkload|staged|}}
{30001002|XXparameter|mssql__interface.condition|param_exact dbms mssql mode output|3|15|P?|@{0|}}
{30001002|XXparameter|mssql__interface_bad_choice||3|2|RF$|Do not use bulkload in versions before V2-14-53|{0|}}
{30001002|XXparameter|mssql__interface_bad_choice.condition|param_exact dbms mssql mode output param_exact mssql__interface bulkload version :2.14.52|3|15|P?|@{0|}}
{30001002|XXparameter|msjet__interface|api|15|1|Fl|Load data using api or utility|{1|api|}}
{30001002|XXparameter|msjet__interface.condition|param_exact dbms msjet mode output|3|15|P?|@{0|}}
{30001002|XXparameter|msaccess__interface|api|15|1|Fl|Load data using api or utility|{1|api|}}
{30001002|XXparameter|msaccess__interface.condition|param_exact dbms msaccess mode output|3|15|P?|@{0|}}
{30001002|XXparameter|oledb__interface|api|15|1|Fl|Load data using api or bulkload|{2|api|bulkload|}}
{30001002|XXparameter|oledb__interface.condition|param_exact dbms oledb mode output|3|15|P?|@{0|}}
{30001002|XXparameter|oledb__interface_bad_choice||3|2|RF$|Do not use bulkload in versions before V2-14-53|{0|}}
{30001002|XXparameter|oledb__interface_bad_choice.condition|param_exact dbms oledb mode output param_exact oledb__interface bulkload version :2.14.52|3|15|P?|@{0|}}
{30001002|XXparameter|ole__interface|sameas mssql__interface sameas msjet__interface sameas msaccess__interface sameas oledb__interface|3|13|v||{0|}}
{30001002|XXparameter|ole__interface.condition|param_exact db_is_ole True mode output|3|15|P?|@{0|}}
{30001002|XXparameter|odbc__interface|api|15|1|Fl|Load data using api|{1|api|}}
{30001002|XXparameter|odbc__interface.condition|param_exact dbms odbc mode output|3|15|P?|@{0|}}
{30001002|XXparameter|greenplum__interface|copy|15|1|Fl|Load data using api or utility|{3|api|copy|gpfdist|}}
{30001002|XXparameter|greenplum__interface.condition|param_exact dbms greenplum mode output|3|15|P?|@{0|}}
{30001002|XXparameter|postgresql__interface|copy|15|1|Fl|Load data using api or utility|{2|api|copy|}}
{30001002|XXparameter|postgresql__interface.condition|param_exact dbms postgresql mode output|3|15|P?|@{0|}}
{30001002|XXparameter|psqlodbc__interface|api|15|1|Fl|For utility loads, manually set the DBMS type parameter on the Data pane to Postgresql, Greenplum or Redshift and press 'NO' when asked if you want to resolve a conflict.  Do NOT change the dbms tag in the dbc file itself.|{1|api|}}
{30001002|XXparameter|psqlodbc__interface.condition|param_exact dbms psqlodbc mode output|3|15|P?|@{0|}}
{30001002|XXparameter|redshift__interface|copy|15|1|Fl|Load data using api or utility|{3|api|copy|copy_via_ssh|}}
{30001002|XXparameter|redshift__interface.condition|param_exact dbms redshift mode output|3|15|P?|@{0|}}
{30001002|XXparameter|sybase__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|sybase__interface.condition|param_exact dbms sybase mode output|3|15|P?|@{0|}}
{30001002|XXparameter|sybaseiq__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|sybaseiq__interface.condition|param_exact dbms sybaseiq mode output|3|15|P?|@{0|}}
{30001002|XXparameter|informix__interface|api|15|1|Fl|Load data using api or utility|{5|api|dbload|dbaccess_xps|pload|dbaccess_serial|}}
{30001002|XXparameter|informix__interface.condition|param_exact dbms informix mode output|3|15|P?|@{0|}}
{30001002|XXparameter|adabas__interface|api|15|1|Fl|Load data using api|{1|api|}}
{30001002|XXparameter|adabas__interface.condition|param_exact dbms adabas mode output|3|15|P?|@{0|}}
{30001002|XXparameter|nza__interface_old|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|nza__interface_old.condition|param_exact dbms netezza mode output version :3.2.3.m99|3|15|P?|@{0|}}
{30001002|XXparameter|nza__interface|external_table|15|1|Fl|Load data using api, utility, or remote external table|{3|api|utility|external_table|}}
{30001002|XXparameter|nza__interface.condition|param_exact dbms netezza mode output version 3.2.4:|3|15|P?|@{0|}}
{30001002|XXparameter|nza__interface_impl|sameas nza__interface sameas nza__interface_old|3|13|v||{0|}}
{30001002|XXparameter|nza__interface_impl.condition|param_exact dbms netezza mode output|3|15|P?|@{0|}}
{30001002|XXparameter|db2_400__interface|api|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|db2_400__interface.condition|param_exact dbms db2_400 mode output|3|15|P?|@{0|}}
{30001002|XXparameter|odbc_400__interface|api|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|odbc_400__interface.condition|param_exact dbms odbc_400 mode output|3|15|P?|@{0|}}
{30001002|XXparameter|aster__interface|api|15|1|Fl|Load data using api or utility|{2|api|ncluster_loader|}}
{30001002|XXparameter|aster__interface.condition|param_exact dbms aster mode output|3|15|P?|@{0|}}
{30001002|XXparameter|snow__interface|api|15|1|Fl|Load data using api mode or files-in-a-stage|{2|api|staged_load|}}
{30001002|XXparameter|snow__interface.condition|param_exact dbms snowflake mode output|3|15|P?|@{0|}}
{30001002|XXparameter|hana__interface|api|15|1|Fl|Load data using api or utility|{1|api|}}
{30001002|XXparameter|hana__interface.condition|param_exact dbms hana mode output version 3.2.5.r24:|3|15|P?|@{0|}}
{30001002|XXparameter|hbase__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|hbase__interface.condition|param_exact dbms hbase mode output version 3.2.5.r24:|3|15|P?|@{0|}}
{30001002|XXparameter|cassandra__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|cassandra__interface.condition|param_exact dbms cassandra mode output version 3.3.2.r8:|3|15|P?|@{0|}}
{30001002|XXparameter|jdbc__interface|api|15|1|Fl|Load data using api or utility|{1|api|}}
{30001002|XXparameter|jdbc__interface.condition|param_exact dbms jdbc mode output version 3.3.2.r8:|3|15|P?|@{0|}}
{30001002|XXparameter|hive__interface|api|15|1|Fl|Load data using api|{1|api|}}
{30001002|XXparameter|hive__interface.condition|param_exact dbms hive mode output version 3.3.4:|3|15|P?|@{0|}}
{30001002|XXparameter|impala__interface|api|15|1|Fl|Load data using api|{1|api|}}
{30001002|XXparameter|impala__interface.condition|param_exact dbms impala mode output version 3.3.5.r19:|3|15|P?|@{0|}}
{30001002|XXparameter|bigquery__interface|utility|15|1|RFl|Load data using api, utility, or storage_load|{3|api|utility|storage_load|}}
{30001002|XXparameter|bigquery__interface.condition|param_exact dbms bigquery mode output version 4.1.0.r1:|3|15|P?|@{0|}}
{30001002|XXparameter|exasol__interface|utility|15|1|RFl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|exasol__interface.condition|param_exact dbms exasol mode output version 4.1.4.r20:|3|15|P?|@{0|}}
{30001002|XXparameter|_ab_param_must_exist|1|3|2|H$||{0|}}
{30001002|XXparameter|not_ims|value dbms ims False value dbms * True default constant True|13|13|v||{0|}}
{30001002|XXparameter|not_ims_or_adabas|value dbms ims False value dbms adabas False value dbms * True default constant True|13|13|v||{0|}}
{30001002|XXparameter|not_unusual_dbms|value dbms ims False value dbms adabas False value dbms mongo False value dbms hbase False value dbms * True default constant True|13|13|v||{0|}}
{30001002|XXparameter|not_iq|value dbms sybaseiq False value dbms * True default constant True|13|13|v||{0|}}
{30001002|XXparameter|not_hbase|value dbms hbase False value dbms * True default constant True|13|13|v||{0|}}
{30001002|XXparameter|not_cassandra|value dbms cassandra False value dbms * True default constant True|13|13|v||{0|}}
{30001002|XXparameter|not_bigquery|value dbms bigquery False value dbms * True default constant True|13|13|v||{0|}}
{30001002|XXparameter|is_iq_util|value not_iq True False value sybaseiq_interface api False default constant True|13|13|v||{0|}}
{30001002|XXparameter|not_ims_or_iqutil|value not_ims False False value is_iq_util True False default constant True|13|13|v||{0|}}
{30001002|XXparameter|supports_num_error|value interface_out api False value dbms netezza True value dbms oracle True value dbms db2eee True value dbms db2ee True value dbms db2 True value dbms db2uss True value dbms sybase True value dbms sybaseiq True value dbms db2_400 True value dbms odbc_400 True value informix__interface dbload True value informix__interface pload True value informix__interface dbaccess_xps True value mssql__interface utility True value dbms cassandra True value exasol__interface utility True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|supports_rows_per_commit|value interface_out api False value interface_out bulkload False value dbms oracle True value dbms mssql True value dbms sybase True value dbms sybaseiq True value informix__interface dbload True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|db2eee_or_db2ee|value dbms db2eee True value dbms db2ee True value dbms db2 True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|supports_native_options|value ora__interface utility True value ora__interface api False value sybase__interface utility True value sybase__interface api False value sybaseiq__interface utility True value sybaseiq__interface api False value nza__interface_impl utility True value nza__interface_impl external_table True value nza__interface_impl api False value ole__interface utility True value ole__interface bulkload False value ole__interface api False value db2ee__interface utility True value db2ee__interface api False value db2eee__interface utility True value db2eee__interface api False value db2__interface utility True value db2__interface api False value db2uss__interface_impl utility True value db2uss__interface_impl api False value db2uss_interface_impl api False value utility DSNTIIAUL False value utility UNLOAD True value nza_interface external_table True value nza_interface api False value cassandra__interface utility True value cassandra__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|supports_control_file|value ora__interface utility True value ora__interface api False value sybase__interface utility True value sybase__interface api False value sybaseiq__interface utility True value sybaseiq__interface api False value ole__interface utility True value ole__interface bulkload False value ole__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|old_ablocal_prerequisite|True|13|1|l||{0|}}
{30001002|XXparameter|old_ablocal_prerequisite.condition|mode input param_exact not_unusual_dbms True param_exact interface api|3|15|P?|@{0|}}
{30001002|XXparameter|supports_ablocal_expr|value nza_interface external_table True value utility DSNTIAUL True value utility UNLOAD False value old_ablocal_prerequisite True True default constant False|13|13|v||{0|}}
{30001002|XXparameter|supports_gen_ldr_datatypes|value db2eee_or_db2ee True True value dbms db2uss True default constant False|13|13|v||{0|}}
{30001002|XXparameter|ftype_pref_nondelimited|value field_type_preference delimited False value field_type_preference fixed True value field_type_preference variable True default constant False|13|13|v||{0|}}
{30001002|XXparameter|supports_batchsize|value ole__interface bulkload True value ole__interface utility False value ole__interface api False value bigquery__interface storage_load True default constant False|13|13|v||{0|}}
{30001002|XXparameter|no_partition_name|True|13|1|l||{0|}}
{30001002|XXparameter|no_partition_name.condition|param_exact dbms sybase version :3.1.1|3|15|P?|@{0|}}
{30001002|XXparameter|supports_partition_name|value ora__interface utility True value ora__interface api False value no_partition_name True False value sybase__interface utility True value sybase__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|supports_reject_threshold|value interface_out api True value mssql__interface bulkload True value bigquery__interface utility True value bigquery__interface storage_load True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|supports_package|value interface_out api True value mssql__interface bulkload True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|supports_logging|value interface_out api True value mssql__interface bulkload True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|supports_logfile|value ter_util True True value interface_out copy True value interface_out copy_via_ssh True value interface_out gpfdist True value interface_out staged_load True value interface_out ncluster_loader True value interface_in staged_unload True value interface_in copy True value interface_in staged True value interface_out staged True value interface_in unload True default constant False|13|13|v||{0|}}
{30001002|XXparameter|supports_api_load_transforms|value interface_out api True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|supports_api_load_transforms.condition|version 3.2.5.r24.0:|3|15|P?|@{0|}}
{30001002|XXparameter|ter_api_encrypt_data|True|13|1|l||{0|}}
{30001002|XXparameter|ter_api_encrypt_data.condition|param_exact dbms teradata version 3.0.2:|3|15|P?|@{0|}}
{30001002|XXparameter|api_encrypt_data_helper|sameas ter_api_encrypt_data default constant False|13|13|v||{0|}}
{30001002|XXparameter|supports_encrypt_data|value api_encrypt_data_helper True True value ter_util True True default constant False|13|13|v||{0|}}
{30001002|XXparameter|interface_in|sameas ora_interface sameas ter_interface sameas db2eee_interface sameas db2ee_interface sameas db2_interface sameas db2uss_interface_impl sameas db2_400_interface sameas ole_interface sameas odbc_interface sameas odbc_400_interface sameas snow_interface sameas sybase_interface sameas sybaseiq_interface sameas informix_interface sameas ims_interface_old sameas ims_interface sameas adabas_interface sameas nza_interface_impl sameas mysql_interface sameas postgresql_interface sameas psqlodbc_interface sameas greenplum_interface sameas redshift_interface sameas vertica_interface sameas aster_interface sameas mongo_interface sameas hana_interface sameas hbase_interface sameas cassandra_interface sameas jdbc_interface sameas hive_interface sameas impala_interface sameas bigquery_interface sameas exasol_interface|3|13|v||{0|}}
{30001002|XXparameter|interface_in.condition|mode input|3|15|P?|@{0|}}
{30001002|XXparameter|interface_out|sameas ora__interface sameas ter__interface sameas db2eee__interface sameas db2ee__interface sameas db2__interface sameas db2uss__interface_impl sameas db2_400__interface sameas ole__interface sameas odbc__interface sameas odbc_400__interface sameas sybase__interface sameas sybaseiq__interface sameas informix__interface sameas adabas__interface sameas nza__interface_impl sameas mysql__interface sameas psqlodbc__interface sameas postgresql__interface sameas greenplum__interface sameas redshift__interface sameas vertica__interface sameas aster__interface sameas snow__interface sameas hana__interface sameas hbase__interface sameas cassandra__interface sameas jdbc__interface sameas hive__interface sameas impala__interface sameas bigquery__interface sameas exasol__interface|3|13|v||{0|}}
{30001002|XXparameter|interface_out.condition|mode output|3|15|P?|@{0|}}
{30001002|XXparameter|interface|sameas interface_in sameas interface_out|3|13|Kv||{0|}}
{30001002|XXparameter|fixed_size_dml|False|13|1|RFOl|Generate fixed size DML when possible|{0|}}
{30001002|XXparameter|fixed_size_dml.condition|param_exact not_ims_or_adabas True version :2.12.2.m4|3|15|P?|@{0|}}
{30001002|XXparameter|field_type_preference|delimited|15|1|RFKl|Format of generated dml|{3|delimited|variable|fixed|}}
{30001002|XXparameter|field_type_preference.condition|param_exact not_unusual_dbms True version 2.12.2.m5:|3|15|P?|@{0|}}
{30001002|XXparameter|generate_loader_datatypes|False|13|1|FKl|Any non-delimited data types in generated DML will be compatible with the utility loader.|{0|}}
{30001002|XXparameter|generate_loader_datatypes.condition|param_exact supports_gen_ldr_datatypes True param_exact ftype_pref_nondelimited True version 2.14.64:|3|15|P?|@{0|}}
{30001002|XXparameter|column_delimiter||3|2|FK$|Default generated column delimiter|{0|}}
{30001002|XXparameter|column_delimiter.condition|param_exact not_unusual_dbms True|3|15|P?|@{0|}}
{30001002|XXparameter|encrypt_data|False|13|1|FKl|Encrypt data between database client and server (if possible)|{0|}}
{30001002|XXparameter|encrypt_data.condition|param_exact supports_encrypt_data True|3|15|P?|@{0|}}
{30001002|XXparameter|dml_with_default_values|none|15|1|RFKl|Generate dml with db default values (if possible)|{3|none|null_preferred|default_preferred|}}
{30001002|XXparameter|dml_with_default_values.condition|param_exact not_unusual_dbms True param_exact not_cassandra True version 3.2.3.r18.0:|3|15|P?|@{0|}}
{30001002|XXparameter|logfile||18|2|FK$|Name of file to store utility log information (optional)|{0|}}
{30001002|XXparameter|logfile.condition|param_exact supports_logfile True|3|15|P?|@{0|}}
{30001002|XXparameter|max_rows||1|2|FK$|Maximum rows to unload|{0|}}
{30001002|XXparameter|max_rows.condition|mode input param_exact not_ims_or_iqutil True|3|15|P?|@{0|}}
{30001002|XXparameter|ablocal_expr||3|2|FK$|DML expression for ABLOCAL() in select|{0|}}
{30001002|XXparameter|ablocal_expr.condition|param_exact supports_ablocal_expr True|3|15|P?|@{0|}}
{30001002|XXparameter|remove_delimiters|False|13|1|RFOl|Remove delimiters found in data|{0|}}
{30001002|XXparameter|remove_delimiters.condition|param_exact interface api mode input version 2.7: param_exact not_unusual_dbms True|3|15|P?|@{0|}}
{30001002|XXparameter|delimiter_replacement||3|2|FK$|Replacement for field delimiters found in data|{0|}}
{30001002|XXparameter|delimiter_replacement.condition|param_exact interface api mode input version 2.7: param_exact not_unusual_dbms True|3|15|P?|@{0|}}
{30001002|XXparameter|non_ansimode||13|1|RFKl|If true, process the select statement using Teradata semantics (like Fast Export/PT Export does); false means use Ansi semantics|{0|}}
{30001002|XXparameter|non_ansimode.condition|param_exact ter_interface api version 2.15.2:|3|15|P?|@{0|}}
{30001002|XXparameter|num_errors|50|1|2|FK$|Maximum number of errors allowed|{0|}}
{30001002|XXparameter|num_errors.condition|mode output param_exact supports_num_error True|3|15|P?|@{0|}}
{30001002|XXparameter|commitNumber||1|2|FK$|Number of records per commit checkpoint|{0|}}
{30001002|XXparameter|commitNumber.condition|mode output param_exact interface api param_exact not_hbase True param_exact not_bigquery True|3|15|P?|@{0|}}
{30001002|XXparameter|commitTable||24|3|FKc|Database table to store commit progress|{0|}}
{30001002|XXparameter|commitTable.condition|mode output param_exact interface api param_exact not_hbase True param_exact not_bigquery True|3|15|P?|@{0|}}
{30001002|XXparameter|jobid||25|3|FKc|For special purpose use only - see 'Help'|{0|}}
{30001002|XXparameter|jobid.condition|mode output param_exact interface api param_exact not_hbase True param_exact not_bigquery True|3|15|P?|@{0|}}
{30001002|XXparameter|ignoreDuplicates|False|13|1|Fl|if true, ignore unique constraint violations|{0|}}
{30001002|XXparameter|ignoreDuplicates.condition|mode output param_exact interface api param_exact not_hbase True param_exact not_cassandra True param_exact not_bigquery True version 2.15:|3|15|P?|@{0|}}
{30001002|XXparameter|no_actions_ok|value ignoreDuplicates True True value ignoreDuplicates False False default constant False|13|13|Kv||{0|}}
{30001002|XXparameter|no_actions_ok.condition|mode output param_exact interface api version 2.15:|3|15|P?|@{0|}}
{30001002|XXparameter|arrayInterfaceBufferSize||1|2|FK$|Size of batch when using array mode|{0|}}
{30001002|XXparameter|arrayInterfaceBufferSize.condition|mode output param_exact interface api param_exact not_bigquery True version 3.0.1:|3|15|P?|@{0|}}
{30001002|XXparameter|rows_per_commit||1|2|FK$|Rows per commit|{0|}}
{30001002|XXparameter|rows_per_commit.condition|mode output param_exact supports_rows_per_commit True|3|15|P?|@{0|}}
{30001002|XXparameter|batch_size|4096|1|2|FK$|Records processed in each batch|{0|}}
{30001002|XXparameter|batch_size.condition|mode output param_exact supports_batchsize True|3|15|P?|@{0|}}
{30001002|XXparameter|is_serial|False|13|1|FKl|Set to true to specify a serial load.|{0|}}
{30001002|XXparameter|is_serial.condition|mode output param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|file_type|delimited|15|1|FKl|Format of data to be loaded. If 'fixed', Output Table reformats the data to the loader's specifications.|{2|delimited|fixed|}}
{30001002|XXparameter|file_type.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True version 2.14.67:|3|15|P?|@{0|}}
{30001002|XXparameter|file_type_modifier||25|3|FKc|Enter desired file type modifiers.|{0|}}
{30001002|XXparameter|file_type_modifier.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|copy_target||25|3|FKc|Refer to DB2 UDB documentation for description|{0|}}
{30001002|XXparameter|copy_target.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|row_count|0|1|2|FK$|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|row_count.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|save_count|0|1|2|FK$|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|save_count.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|data_buffer_size|0|1|2|FK$|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|data_buffer_size.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|sort_buffer_size|0|1|2|FK$|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|sort_buffer_size.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|hold_quiesce|False|13|1|FKl|Set to True to leave the utility in quiesced exclusive state.|{0|}}
{30001002|XXparameter|hold_quiesce.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|cpu_parallelism|1|1|2|FK$|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|cpu_parallelism.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|disk_parallelism|0|1|2|FK$|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|disk_parallelism.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|recoverable|False|13|1|FKl|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|recoverable.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|indexing_mode|autoselect|15|1|FKl|Refer to DB2 UDB documentation for description.|{4|autoselect|rebuild|incremental|deferred|}}
{30001002|XXparameter|indexing_mode.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|access_level|allow_no_access|15|1|FKl|Refer to DB2 UDB documentation for description.|{2|allow_no_access|allow_read_access|}}
{30001002|XXparameter|access_level.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|use_tablespace||25|3|FKc|Refer to DB2 UDB documentation for description|{0|}}
{30001002|XXparameter|use_tablespace.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True version 2.14.101:|3|15|P?|@{0|}}
{30001002|XXparameter|lock_with_force|False|13|1|FKl|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|lock_with_force.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|check_pending|deferred|15|1|FKl|Refer to DB2 UDB documentation for description.|{2|immediate|deferred|}}
{30001002|XXparameter|check_pending.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|statsopt|none|15|1|FKl|Refer to DB2 UDB documentation for description.|{2|none|use_profile|}}
{30001002|XXparameter|statsopt.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|use_exception_table|False|13|1|FKl|Set to True to use exception table.|{0|}}
{30001002|XXparameter|use_exception_table.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|exception_table||25|3|FKc|Use only if you want to explicity specify the name for the exception table.|{0|}}
{30001002|XXparameter|exception_table.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|aster_dummy|a|3|2|$||{0|}}
{30001002|XXparameter|aster_util_in|value aster_interface api False value aster_interface * True default constant False|13|13|v||{0|}}
{30001002|XXparameter|aster_util_in.condition|param_exact dbms aster mode input|3|15|P?|@{0|}}
{30001002|XXparameter|aster_util_out|value aster__interface api False value aster__interface * True default constant False|13|13|v||{0|}}
{30001002|XXparameter|aster_util_out.condition|param_exact dbms aster mode output|3|15|P?|@{0|}}
{30001002|XXparameter|aster_util|value aster_util_in True True value aster_util_out True True value aster_dummy * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|aster_util.condition|param_exact dbms aster|3|15|P?|@{0|}}
{30001002|XXparameter|uses_legacy_inheritance|False|13|1|FKl|Set to True if table uses legacy parent/child inheritance |{0|}}
{30001002|XXparameter|uses_legacy_inheritance.condition|param_exact aster_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|allow_errors|False|13|1|FKl|If true, write errors to error table OR (if false) abort load on first error|{0|}}
{30001002|XXparameter|allow_errors.condition|param_exact aster_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|error_table||3|2|FK$|User-specified errtable name (optional)|{0|}}
{30001002|XXparameter|error_table.condition|param_exact allow_errors True|3|15|P?|@{0|}}
{30001002|XXparameter|label||3|2|FK$|Optional string to identify errors from this load in error table|{0|}}
{30001002|XXparameter|label.condition|param_exact allow_errors True|3|15|P?|@{0|}}
{30001002|XXparameter|like_postg|value dbms postgresql True value dbms greenplum True value dbms redshift True value dbms Redshift True default constant False|13|13|v||{0|}}
{30001002|XXparameter|db_type|sameas dbms|3|13|Kv|psqlodbc needs this param set|{0|}}
{30001002|XXparameter|db_type.condition|param_exact like_postg True|3|15|P?|@{0|}}
{30001002|XXparameter|gpfdist_log_prefix||18|2|FK$|Use this to override the log file path prefix in the gpdfdist configuration file (optional)|{0|}}
{30001002|XXparameter|gpfdist_log_prefix.condition|param_exact greenplum__interface gpfdist|3|15|P?|@{0|}}
{30001002|XXparameter|stage_name||3|2|FK$|optional name of stage.  If specified, MUST be in format defined for dbc file's default_stage parameter.|{0|}}
{30001002|XXparameter|stage_name.condition|param_exact dbms snowflake param_exact interface staged_*|3|15|P?|@{0|}}
{30001002|XXparameter|put_parallel_threads||1|2|FK$|Number of threads to use with PUT command|{0|}}
{30001002|XXparameter|put_parallel_threads.condition|param_exact dbms snowflake param_exact interface staged_load|3|15|P?|@{0|}}
{30001002|XXparameter|max_file_size|2000000|3|2|FK$|optional maximum size per unloaded file.|{0|}}
{30001002|XXparameter|max_file_size.condition|param_exact dbms snowflake param_exact interface staged_unload|3|15|P?|@{0|}}
{30001002|XXparameter|inf_dummy|a|3|2|$||{0|}}
{30001002|XXparameter|inf_util_in|value informix_interface api False value informix_interface * True default constant False|13|13|v||{0|}}
{30001002|XXparameter|inf_util_in.condition|param_exact dbms informix mode input|3|15|P?|@{0|}}
{30001002|XXparameter|inf_util_out|value informix__interface api False value informix__interface * True default constant False|13|13|v||{0|}}
{30001002|XXparameter|inf_util_out.condition|param_exact dbms informix mode output|3|15|P?|@{0|}}
{30001002|XXparameter|inf_util|value inf_util_in True True value inf_util_out True True value inf_dummy * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|inf_util.condition|param_exact dbms informix|3|15|P?|@{0|}}
{30001002|XXparameter|inf_use_rej|value informix__interface dbaccess_xps True value informix__interface dbload True value informix__interface pload True value inf_dummy * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|inf_use_rej.condition|param_exact inf_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|idelim|\||3|2|FK$|One-character delimiter for Informix utilities|{0|}}
{30001002|XXparameter|idelim.condition|param_exact inf_util True|3|15|P?|@{0|}}
{30001002|XXparameter|pdqpriority||3|2|FK$|DEFAULT, LOW, HIGH, ...|{0|}}
{30001002|XXparameter|pdqpriority.condition|param_exact inf_util True|3|15|P?|@{0|}}
{30001002|XXparameter|jobname||3|2|RFK$|Jobname for High-performance Load/Unload|{0|}}
{30001002|XXparameter|jobname.condition|param_exact interface pload|3|15|P?|@{0|}}
{30001002|XXparameter|pload_job_method|new|15|1|RFKl||{3|new|overwrite|reuse|}}
{30001002|XXparameter|pload_job_method.condition|param_exact interface pload|3|15|P?|@{0|}}
{30001002|XXparameter|isolation||3|2|FK$|'set isolation' choices: DIRTY READ, COMMITTED READ, ...|{0|}}
{30001002|XXparameter|isolation.condition|param_exact inf_util_in True|3|15|P?|@{0|}}
{30001002|XXparameter|ansi_isolation|default|15|1|FKl|'set transaction' isolation choices|{5|default|READ COMMITTED|REPEATABLE READ|SERIALIZABLE|READ UNCOMMITTED|}}
{30001002|XXparameter|ansi_isolation.condition|param_exact inf_util_in True|3|15|P?|@{0|}}
{30001002|XXparameter|force_raw|False|13|1|FKl|If true, temporarily alters table log mode to RAW|{0|}}
{30001002|XXparameter|force_raw.condition|param_exact inf_util_out True param_exact informix__interface dbaccess_xps|3|15|P?|@{0|}}
{30001002|XXparameter|rejectfile||3|2|RFK$|Rejectfile specification for Informix load utility|{0|}}
{30001002|XXparameter|rejectfile.condition|param_exact inf_use_rej True|3|15|P?|@{0|}}
{30001002|XXparameter|dummy1|a|3|2|$||{0|}}
{30001002|XXparameter|ter_util_in|value ter_interface api False value ter_interface * True default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_util_in.condition|param_exact dbms teradata mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ter_util_out|value ter__interface api False value ter__interface * True default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_util_out.condition|param_exact dbms teradata mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ter_util|value ter_util_in True True value ter_util_out True True value dummy1 * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_util.condition|param_exact dbms teradata|3|15|P?|@{0|}}
{30001002|XXparameter|ter_wb_in|value ter_interface WB_* True value ter_interface PT_* True value ter_interface * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_wb_in.condition|param_exact dbms teradata mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ter_wb_out|value ter__interface WB_* True value ter__interface PT_* True value ter__interface * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_wb_out.condition|param_exact dbms teradata mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ter_wb|value ter_wb_in True True value ter_wb_out True True value dummy1 * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_wb.condition|param_exact dbms teradata|3|15|P?|@{0|}}
{30001002|XXparameter|ter_is_fload|value ter__interface Fastload True value ter__interface WB_load True value ter__interface PT_load True value ter__interface PT_api_load True value ter__interface AB_Fastload True value ter__interface * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_is_fload.condition|param_exact dbms teradata mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ter_is_tpump|value ter__interface Tpump True value ter__interface WB_stream True value ter__interface PT_stream True value ter__interface PT_api_stream True value ter__interface * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_is_tpump.condition|param_exact dbms teradata mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ter_m_or_t|value ter__interface PT_inserter False value ter_is_fload False True value ter_is_fload True False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_m_or_t.condition|param_exact ter_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|ter_is_mload_only|value ter__interface Multiload True value ter__interface * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_is_mload_only.condition|param_exact dbms teradata mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ter_show_brief|value ter_m_or_t True True value ter_util_in True True value dummy1 * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ter_show_brief.condition|param_exact dbms teradata|3|15|P?|@{0|}}
{30001002|XXparameter|brief|True|13|1|FKl|Ask utility to generate brief log info if TRUE|{0|}}
{30001002|XXparameter|brief.condition|param_exact ter_show_brief True|3|15|P?|@{0|}}
{30001002|XXparameter|sleep|6|1|2|FK$|Minutes to wait before retrying logon|{0|}}
{30001002|XXparameter|sleep.condition|param_exact ter_util True|3|15|P?|@{0|}}
{30001002|XXparameter|tenacity|0|1|2|FK$|Maximum hours to attempt logon before utility gives up|{0|}}
{30001002|XXparameter|tenacity.condition|param_exact ter_util True|3|15|P?|@{0|}}
{30001002|XXparameter|sessions||3|2|FK$|Number of  sessions|{0|}}
{30001002|XXparameter|sessions.condition|param_exact ter_util True|3|15|P?|@{0|}}
{30001002|XXparameter|instances|1|1|2|FK$|Number of Parallel Transporter operator instances|{0|}}
{30001002|XXparameter|instances.condition|param_exact ter_wb True|3|15|P?|@{0|}}
{30001002|XXparameter|logtab_name||3|2|FK$|User-specified logtable name (optional)|{0|}}
{30001002|XXparameter|logtab_name.condition|param_exact ter_util True|3|15|P?|@{0|}}
{30001002|XXparameter|no_spool|False|13|1|FKl|If True, do not spool output if possible|{0|}}
{30001002|XXparameter|no_spool.condition|param_exact ter_util_in True version 3.0.1:|3|15|P?|@{0|}}
{30001002|XXparameter|use_load_isolation|False|13|1|FKl|Use Teradata load isolation capability when loading data|{0|}}
{30001002|XXparameter|use_load_isolation.condition|param_exact ter_is_tpump True|3|15|P?|@{0|}}
{30001002|XXparameter|tablewait|0|1|2|FK$|Number of hours to wait on other load tasks before utility gives up|{0|}}
{30001002|XXparameter|tablewait.condition|param_exact ter_is_mload_only True|3|15|P?|@{0|}}
{30001002|XXparameter|vartext|False|13|1|FKl|Send data to Teradata load utility in VARTEXT mode|{0|}}
{30001002|XXparameter|vartext.condition|param_exact ter_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|delim|\||3|2|FK$|delimiter for Teradata load utility|{0|}}
{30001002|XXparameter|delim.condition|param_exact vartext True|3|15|P?|@{0|}}
{30001002|XXparameter|np_axsmod_dir||26|2|RFK$|Name of directory for Named Pipe AXSMOD storage (required)|{0|}}
{30001002|XXparameter|np_axsmod_dir.condition|param_exact ter_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|checkpoint||1|2|FK$|Checkpoint rate to be used by Teradata utility|{0|}}
{30001002|XXparameter|checkpoint.condition|param_exact ter_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|worktab_name||3|2|FK$|User-specified worktable name (optional)|{0|}}
{30001002|XXparameter|worktab_name.condition|param_exact ter_util_out True param_exact ter_m_or_t True param_exact ter_is_tpump False|3|15|P?|@{0|}}
{30001002|XXparameter|errtab_name||3|2|FK$|User-specified errtable name (optional) [will have 1,2.. appended]|{0|}}
{30001002|XXparameter|errtab_name.condition|param_exact ter_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|macrodb||3|2|FK$|Database to use for Tpump macro creation and usage|{0|}}
{30001002|XXparameter|macrodb.condition|param_exact ter_is_tpump True|3|15|P?|@{0|}}
{30001002|XXparameter|errlimit||1|2|FK$|number errors allowed|{0|}}
{30001002|XXparameter|errlimit.condition|param_exact ter_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|errpercent||1|2|FK$|% errors allowed|{0|}}
{30001002|XXparameter|errpercent.condition|param_exact ter_m_or_t True|3|15|P?|@{0|}}
{30001002|XXparameter|ignore_dupl_rows|False|13|1|FKl|If true, ignore duplicate rows on insert|{0|}}
{30001002|XXparameter|ignore_dupl_rows.condition|param_exact ter_m_or_t True|3|15|P?|@{0|}}
{30001002|XXparameter|serialize|False|13|1|FKl|Guarantee serializability of operations on the same key if true|{0|}}
{30001002|XXparameter|serialize.condition|param_exact ter_is_tpump True|3|15|P?|@{0|}}
{30001002|XXparameter|pack|1|1|2|FK$|# of records to pack into a Tpump request or a TPT Update buffer|{0|}}
{30001002|XXparameter|pack.condition|param_exact ter_m_or_t True|3|15|P?|@{0|}}
{30001002|XXparameter|rate||1|2|FK$|# of statements per minute to send to Teradata database|{0|}}
{30001002|XXparameter|rate.condition|param_exact ter_is_tpump True|3|15|P?|@{0|}}
{30001002|XXparameter|trace_directory||26|2|FK$|directory for trace/debug info (optional)|{0|}}
{30001002|XXparameter|trace_directory.condition|param_exact ter_util True|3|15|P?|@{0|}}
{30001002|XXparameter|axsmod_tracelevel|None|3|2|FK$|for axsmod debugging|{0|}}
{30001002|XXparameter|axsmod_tracelevel.condition|param_exact ter_util True|3|15|P?|@{0|}}
{30001002|XXparameter|pt_tracelevel|None|3|2|FK$|for Parallel Transporter (was TWB) debugging|{0|}}
{30001002|XXparameter|pt_tracelevel.condition|param_exact ter_wb True|3|15|P?|@{0|}}
{30001002|XXparameter|ora_util_out|value ora__interface utility True value ora__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ora_util_out.condition|param_exact dbms oracle mode output|3|15|P?|@{0|}}
{30001002|XXparameter|oracle_max_kb||1|2|FK$|max kilobytes for each parallel unload select|{0|}}
{30001002|XXparameter|oracle_max_kb.condition|param_exact dbms oracle mode input version 2.10.12.m12:|3|15|P?|@{0|}}
{30001002|XXparameter|parallel_mode|rowid|15|1|RFKl|method of automatic parallelism|{3|rowid|subpartition|partition|}}
{30001002|XXparameter|parallel_mode.condition|param_exact dbms oracle mode input version 2.14:|3|15|P?|@{0|}}
{30001002|XXparameter|include_partitions||17|2|FKf$|Optional list of partitions to extract|{0|}}
{30001002|XXparameter|include_partitions.condition|param_exact dbms oracle mode input version 2.15.4.r30:|3|15|P?|@{0|}}
{30001002|XXparameter|flashback_queries|False|13|1|FKl|Employ Oracle flashback queries|{0|}}
{30001002|XXparameter|flashback_queries.condition|param_exact dbms oracle mode input version 3.2.6.r28:|3|15|P?|@{0|}}
{30001002|XXparameter|system_change_number||1|2|FK$|optional System Change Number|{0|}}
{30001002|XXparameter|system_change_number.condition|param_exact flashback_queries True version 3.2.6.r28:|3|15|P?|@{0|}}
{30001002|XXparameter|direct|True|13|1|RFOl|Use SQL*Loader Direct Mode|{0|}}
{30001002|XXparameter|direct.condition|param_exact ora_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|num_discards||1|2|FK$|# discards allowed|{0|}}
{30001002|XXparameter|num_discards.condition|param_exact ora_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|bindsize||1|2|FK$|SQL*Loader BINDSIZE setting|{0|}}
{30001002|XXparameter|bindsize.condition|param_exact ora_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|control_file||17|2|FKf$|Optional database load utility control file|{0|}}
{30001002|XXparameter|control_file.condition|param_exact supports_control_file True version 2.7:|3|15|P?|@{0|}}
{30001002|XXparameter|partition_name||3|2|FK$|Database partition name|{0|}}
{30001002|XXparameter|partition_name.condition|param_exact supports_partition_name True|3|15|P?|@{0|}}
{30001002|XXparameter|native_options||25|3|FKc|Optional commands passed directly to the database load utility.|{0|}}
{30001002|XXparameter|native_options.condition|param_exact supports_native_options True|3|15|P?|@{0|}}
{30001002|XXparameter|syb_util_out|value sybase__interface utility True value sybase__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|syb_util_out.condition|param_exact dbms sybase mode output|3|15|P?|@{0|}}
{30001002|XXparameter|sybaseiq_util_out|value sybaseiq__interface utility True value sybaseiq__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|sybaseiq_util_out.condition|param_exact dbms sybaseiq mode output|3|15|P?|@{0|}}
{30001002|XXparameter|row_delimited_by|if appropriate|15|1|FKl|Generate ROW DELIMITED BY clause in control file when appropriate.|{2|if appropriate|never|}}
{30001002|XXparameter|row_delimited_by.condition|param_exact sybaseiq_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|quoted_identifiers|always|15|1|FKl|Quote column names in generated control file.|{2|always|never|}}
{30001002|XXparameter|quoted_identifiers.condition|param_exact sybaseiq_util_out True version :3.0.3.r14|3|15|P?|@{0|}}
{30001002|XXparameter|supports_constraint|True|13|1|Kl||{0|}}
{30001002|XXparameter|supports_constraint.condition|param_exact sybaseiq_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|unique_constraint|True|13|1|FKl|If True, include UNIQUE as a load constraint to ignore.|{0|}}
{30001002|XXparameter|unique_constraint.condition|param_exact sybaseiq_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|null_constraint|True|13|1|FKl|If True, include NULL as a load constraint to ignore.|{0|}}
{30001002|XXparameter|null_constraint.condition|param_exact sybaseiq_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|foreign_key_constraint|False|13|1|FKl|If True, include FOREIGN KEY as a load constraint to ignore.|{0|}}
{30001002|XXparameter|foreign_key_constraint.condition|param_exact sybaseiq_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|data_value_constraint|False|13|1|FKl|If True, include DATA VALUE as a load constraint to ignore.|{0|}}
{30001002|XXparameter|data_value_constraint.condition|param_exact sybaseiq_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|ims_api_in|value ims_interface_old api True value ims_interface api True value ims_interface ims_connect False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ims_api_in.condition|param_exact dbms ims mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ims_remote_in|value ims_interface ims_connect True value ims_interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|ims_remote_in.condition|param_exact dbms ims mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ims_api_or_remote_in|value ims_api_in True True value ims_remote_in True True default constant False|13|13|v||{0|}}
{30001002|XXparameter|batch_mode|DLI|15|1|RFKl|Run standalone batch or in a batch message processing region|{2|DLI|BMP|}}
{30001002|XXparameter|batch_mode.condition|param_exact ims_api_in True|3|15|P?|@{0|}}
{30001002|XXparameter|parent_segment_data|key_only|15|1|RFKl|Unload just the keys from parents, or all the data|{2|key_only|full_segment|}}
{30001002|XXparameter|parent_segment_data.condition|param_exact ims_api_or_remote_in True|3|15|P?|@{0|}}
{30001002|XXparameter|generate_unique_key|False|13|1|FKl|Generate a unique for segments with no unique key|{0|}}
{30001002|XXparameter|generate_unique_key.condition|param_exact ims_api_or_remote_in True|3|15|P?|@{0|}}
{30001002|XXparameter|_ab_ims_path_command|m_db ims_path|3|2|$|Command to generate list of segments|{0|}}
{30001002|XXparameter|_ab_ims_path_command_params|config_file positional table_spec keyword|3|2|$|List of parameters for path command|{0|}}
{30001002|XXparameter|ims_path|generated _ab_ims_path_command page_change|3|13|FHv|List of segments from root to segment|{0|}}
{30001002|XXparameter|ims_path.is_index|true|3|9|P|@{0|}}
{30001002|XXparameter|ims_path.condition|param_exact dbms ims mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ssa_||3|2|FK$|SSA qualifier for this segment|{0|}}
{30001002|XXparameter|ssa_.index|ims_path|3|9|P|@{0|}}
{30001002|XXparameter|ssa_.index_name|ssa_|3|9|P|@{0|}}
{30001002|XXparameter|ssa_.condition|param_exact dbms ims mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ims_parameters||3|2|FK$|Other ims parameters|{0|}}
{30001002|XXparameter|ims_parameters.condition|param_exact ims_api_in True|3|15|P?|@{0|}}
{30001002|XXparameter|alternate_psbname||3|2|FK$|Alternate PSB name to use when starting DFSRRC00|{0|}}
{30001002|XXparameter|alternate_psbname.condition|param_exact ims_api_in True|3|15|P?|@{0|}}
{30001002|XXparameter|ims_batch_size|10|1|2|RFK$|Number of records to request per call|{0|}}
{30001002|XXparameter|ims_batch_size.condition|param_exact ims_remote_in True|3|15|P?|@{0|}}
{30001002|XXparameter|file_number||1|2|FK$|Set to valid file number to bypass PREDICT for file number lookup.|{0|}}
{30001002|XXparameter|file_number.condition|param_exact dbms adabas|3|15|P?|@{0|}}
{30001002|XXparameter|backup_dataset||3|2|FK$|DSN of backup tape|{0|}}
{30001002|XXparameter|backup_dataset.condition|mode input param_exact dbms adabas param_exact adabas_interface backup_tape|3|15|P?|@{0|}}
{30001002|XXparameter|file_password||3|2|FK$|Adabas password in encrypted form (m_passwd).|{0|}}
{30001002|XXparameter|file_password.condition|param_exact dbms adabas|3|15|P?|@{0|}}
{30001002|XXparameter|nza_util_out|value nza__interface_impl utility True value nza__interface_impl external_table True value nza__interface_impl api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|nza_util_out.condition|param_exact dbms netezza mode output|3|15|P?|@{0|}}
{30001002|XXparameter|file_format|text|15|1|FKl|Format of data to be loaded. If 'fixed', Output Table reformats the data.|{2|text|fixed|}}
{30001002|XXparameter|file_format.condition|mode output param_exact nza__interface_impl external_table version 4.1.1.r9:|3|15|P?|@{0|}}
{30001002|XXparameter|local_log_path||3|2|FK$|local path to log directory mapped from NPS server|{0|}}
{30001002|XXparameter|local_log_path.condition|version :3.2.0|3|15|P?|@{0|}}
{30001002|XXparameter|server_log_path||3|2|FK$|path on NPS server to log directory|{0|}}
{30001002|XXparameter|server_log_path.condition|version :3.2.0|3|15|P?|@{0|}}
{30001002|XXparameter|db2uss_util_out|value db2uss__interface_impl utility True value db2uss__interface_impl api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|db2uss_util_out.condition|param_exact dbms db2uss mode output|3|15|P?|@{0|}}
{30001002|XXparameter|utility|DSNTIAUL|15|1|FKl|IBM utility used to unload the database|{2|DSNTIAUL|UNLOAD|}}
{30001002|XXparameter|utility.condition|mode input param_exact db2uss_interface_impl utility version 2.13.1:|3|15|P?|@{0|}}
{30001002|XXparameter|supports_utility_id|value db2uss_util_out True True value utility UNLOAD True value utility DSNTIAUL False default constant False|13|13|v||{0|}}
{30001002|XXparameter|utility_ID||3|2|FK$|Unique ID for DB2 utility job|{0|}}
{30001002|XXparameter|utility_ID.condition|param_exact interface utility param_exact supports_utility_id True version 2.13.1:|3|15|P?|@{0|}}
{30001002|XXparameter|temp_dataset||28|2|FK$|MVS dataset for UNLOAD output|{0|}}
{30001002|XXparameter|temp_dataset.condition|mode input param_exact db2uss_interface_impl utility param_exact utility UNLOAD version 2.13.1:|3|15|P?|@{0|}}
{30001002|XXparameter|image_copy_dataset||28|2|FK$|Optional DB/2 image copy dataset or datasets from which to unload data|{0|}}
{30001002|XXparameter|image_copy_dataset.condition|mode input param_exact db2uss_interface_impl utility param_exact utility UNLOAD version 2.14.68:|3|15|P?|@{0|}}
{30001002|XXparameter|tablespace||3|2|FK$|If unloading from an image copy dataset, the name of the tablespace which contains the data from the target table|{0|}}
{30001002|XXparameter|tablespace.condition|mode input param_exact db2uss_interface_impl utility param_exact utility UNLOAD version 2.14.68:|3|15|P?|@{0|}}
{30001002|XXparameter|partition||3|2|FK$|If unloading from a partitioned table, the partition number or range to unload|{0|}}
{30001002|XXparameter|partition.condition|mode input param_exact db2uss_interface_impl utility param_exact utility UNLOAD version 3.5.2.r9:|3|15|P?|@{0|}}
{30001002|XXparameter|SYSREC_dataset||28|2|FK$|MVS dataset for utility input|{0|}}
{30001002|XXparameter|SYSREC_dataset.condition|param_exact db2uss_util_out True version 2.13.1:|3|15|P?|@{0|}}
{30001002|XXparameter|SYSDISC_dataset||28|2|FK$|Optional MVS dataset for utility rejects|{0|}}
{30001002|XXparameter|SYSDISC_dataset.condition|param_exact db2uss_util_out True version 2.13.1:|3|15|P?|@{0|}}
{30001002|XXparameter|SYSERR_dataset||28|2|FK$|Optional MVS dataset for utility error output|{0|}}
{30001002|XXparameter|SYSERR_dataset.condition|param_exact db2uss_util_out True version 2.13.1:|3|15|P?|@{0|}}
{30001002|XXparameter|SYSUT1_dataset||28|2|FK$|Optional MVS work dataset for sort input|{0|}}
{30001002|XXparameter|SYSUT1_dataset.condition|param_exact db2uss_util_out True version 2.13.1:|3|15|P?|@{0|}}
{30001002|XXparameter|SORTOUT_dataset||28|2|FK$|Optional MVS work dataset for sort output|{0|}}
{30001002|XXparameter|SORTOUT_dataset.condition|param_exact db2uss_util_out True version 2.13.1:|3|15|P?|@{0|}}
{30001002|XXparameter|SYSMAP_dataset||28|2|FK$|Optional MVS work dataset for utility error processing|{0|}}
{30001002|XXparameter|SYSMAP_dataset.condition|param_exact db2uss_util_out True version 2.13.1:|3|15|P?|@{0|}}
{30001002|XXparameter|SYSCOPY_dataset||28|2|FK$|Optional MVS dataset for an inline image copy|{0|}}
{30001002|XXparameter|SYSCOPY_dataset.condition|param_exact db2uss_util_out True version 3.5.3.r11:|3|15|P?|@{0|}}
{30001002|XXparameter|use_mssql_bulk_out|value ole__interface bulkload True value ole__interface utility False value ole__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|staged_load_unload|value mssql__interface staged True value mssql__interface utility False value mssql__interface bulkload False value mssql__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|keep_identity|False|13|1|FKl|When "True" user must provide values for identity columns.|{0|}}
{30001002|XXparameter|keep_identity.condition|param_exact use_mssql_bulk_out True|3|15|P?|@{0|}}
{30001002|XXparameter|keep_nulls|False|13|1|FKl|When "true" a null input value will store a null, not the column default value.|{0|}}
{30001002|XXparameter|keep_nulls.condition|param_exact use_mssql_bulk_out True|3|15|P?|@{0|}}
{30001002|XXparameter|hints||3|2|FK$|The same as the "-h" option to bcp.|{0|}}
{30001002|XXparameter|hints.condition|param_exact use_mssql_bulk_out True|3|15|P?|@{0|}}
{30001002|XXparameter|storage_type|blob|15|1|FKl|Type of Azure data storage used for the staging area. blob - Azure Blob Storage; adlsg2 - Azure Data Lake Storage Gen2|{2|blob|adlsg2|}}
{30001002|XXparameter|storage_type.condition|param_exact staged_load_unload True|3|15|P?|@{0|}}
{30001002|XXparameter|as400_util_out|value db2_400__interface utility True value db2_400__interface api False value odbc_400__interface utility True value odbc_400__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|hostname||3|2|FK$|Name of AS/400 system|{0|}}
{30001002|XXparameter|hostname.condition|param_exact as400_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|rebuild_loader|False|13|1|FKl|If True, rebuild the RPG table loader program on the AS/400.|{0|}}
{30001002|XXparameter|rebuild_loader.condition|param_exact as400_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|ifs_data_path||3|2|FK$|Location, on the AS/400, where data will be landed|{0|}}
{30001002|XXparameter|ifs_data_path.condition|param_exact as400_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|ifs_dml_path||3|2|FK$|Location, on the AS/400, where dml files will be landed|{0|}}
{30001002|XXparameter|ifs_dml_path.condition|param_exact as400_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|mongo_api_in|value mongo_interface api True default constant False|13|13|v||{0|}}
{30001002|XXparameter|mongo_api_in.condition|param_exact dbms mongo mode input|3|15|P?|@{0|}}
{30001002|XXparameter|continue_on_bad_type|False|13|1|RFKl|False will abort if a Mongo document is read which cannot be translated into the port DML.  True will skip the document and continue processing.|{0|}}
{30001002|XXparameter|continue_on_bad_type.condition|param_exact mongo_api_in True version 3.2.3.r18:|3|15|P?|@{0|}}
{30001002|XXparameter|hbase_api|value interface_out utility False value dbms hbase True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|hbase_utility|value interface_out utility True value dbms hbase True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|filterstring||3|2|FK$|Thrift filterstring|{0|}}
{30001002|XXparameter|filterstring.condition|param_exact dbms hbase mode input|3|15|P?|@{0|}}
{30001002|XXparameter|columns||3|2|FK$|List of column names to load or unload; names are <family>:<qualifier> and separated by spaces|{0|}}
{30001002|XXparameter|columns.condition|param_exact dbms hbase param_exact hbase_api True|3|15|P?|@{0|}}
{30001002|XXparameter|timestamp||1|2|FK$|target timestamp (8-byte signed integer)|{0|}}
{30001002|XXparameter|timestamp.condition|param_exact dbms hbase|3|15|P?|@{0|}}
{30001002|XXparameter|start_rowkey||3|2|FK$|lower rowkey bound (inclusive) to unload|{0|}}
{30001002|XXparameter|start_rowkey.condition|param_exact dbms hbase mode input|3|15|P?|@{0|}}
{30001002|XXparameter|stop_rowkey||3|2|FK$|upper rowkey bound (exclusive) to unload|{0|}}
{30001002|XXparameter|stop_rowkey.condition|param_exact dbms hbase mode input|3|15|P?|@{0|}}
{30001002|XXparameter|skip_WAL|False|13|1|FKl|omit write to HBase WAL|{0|}}
{30001002|XXparameter|skip_WAL.condition|param_exact dbms hbase mode output param_exact hbase_api True|3|15|P?|@{0|}}
{30001002|XXparameter|sort_rowkeys|True|13|1|FKl|Sort incoming record stream on rowkey|{0|}}
{30001002|XXparameter|sort_rowkeys.condition|param_exact dbms hbase mode output|3|15|P?|@{0|}}
{30001002|XXparameter|sort_columns|True|13|1|FKl|Sort value vectors in each record on family and column names|{0|}}
{30001002|XXparameter|sort_columns.condition|param_exact dbms hbase mode output|3|15|P?|@{0|}}
{30001002|XXparameter|default_family||3|2|FK$|default column_family (used for flat record format only)|{0|}}
{30001002|XXparameter|default_family.condition|param_exact dbms hbase|3|15|P?|@{0|}}
{30001002|XXparameter|rowkey_field||3|2|FK$|field name for rowkey (used for flat record format only)|{0|}}
{30001002|XXparameter|rowkey_field.condition|param_exact dbms hbase|3|15|P?|@{0|}}
{30001002|XXparameter|tmp_file_mode|660|3|2|FK$|octal file mode must allow read by HBase server|{0|}}
{30001002|XXparameter|tmp_file_mode.condition|param_exact dbms hbase param_exact hbase_utility True param_exact hbase_api False|3|15|P?|@{0|}}
{30001002|XXparameter|hbase_configfile||3|2|FK$|optional configuration file used by HBase for load|{0|}}
{30001002|XXparameter|hbase_configfile.condition|param_exact dbms hbase param_exact hbase_utility True param_exact hbase_api False|3|15|P?|@{0|}}
{30001002|XXparameter|cassandra_api|value interface_out utility False value dbms cassandra True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|cassandra_utility|value interface_out utility True value dbms cassandra True value dbms * False default constant True|13|13|v||{0|}}
{30001002|XXparameter|chunksize||1|2|FK$|Chunk size for worker processes|{0|}}
{30001002|XXparameter|chunksize.condition|param_exact dbms cassandra mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ingestrate||1|2|FK$|Approx. ingest rate|{0|}}
{30001002|XXparameter|ingestrate.condition|param_exact dbms cassandra mode output|3|15|P?|@{0|}}
{30001002|XXparameter|max_batchsize||1|2|FK$|Max size of import batch|{0|}}
{30001002|XXparameter|max_batchsize.condition|param_exact dbms cassandra mode output|3|15|P?|@{0|}}
{30001002|XXparameter|min_batchsize||1|2|FK$|Min size of import batch|{0|}}
{30001002|XXparameter|min_batchsize.condition|param_exact dbms cassandra mode output|3|15|P?|@{0|}}
{30001002|XXparameter|num_processes||1|2|FK$|Number of worker processes|{0|}}
{30001002|XXparameter|num_processes.condition|param_exact dbms cassandra mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ttl_seconds||1|2|FK$|Time to live in seconds|{0|}}
{30001002|XXparameter|ttl_seconds.condition|param_exact dbms cassandra mode output|3|15|P?|@{0|}}
{30001002|XXparameter|bq_util_in|value bigquery_interface utility True value bigquery_interface * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|bq_util_in.condition|param_exact dbms bigquery mode input|3|15|P?|@{0|}}
{30001002|XXparameter|bq_util_out|value bigquery__interface utility True value bigquery__interface * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|bq_util_out.condition|param_exact dbms bigquery mode output|3|15|P?|@{0|}}
{30001002|XXparameter|bq_util|value bq_util_in True True value bq_util_out True True default constant False|13|13|v||{0|}}
{30001002|XXparameter|bq_storage_in|value bigquery_interface storage_extract True value bigquery_interface * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|bq_storage_in.condition|param_exact dbms bigquery mode input|3|15|P?|@{0|}}
{30001002|XXparameter|bq_storage_out|value bigquery__interface storage_load True value bigquery__interface * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|bq_storage_out.condition|param_exact dbms bigquery mode output|3|15|P?|@{0|}}
{30001002|XXparameter|bq_storage|value bq_storage_in True True value bq_storage_out True True default constant False|13|13|v||{0|}}
{30001002|XXparameter|mux_config_file||17|2|FKf$|XML configuration file for the multiplexer used by this component|{0|}}
{30001002|XXparameter|mux_config_file.condition|param_exact bq_util True version 4.2.0.r1:|3|15|P?|@{0|}}
{30001002|XXparameter|schema_file||3|2|FK$|Avro schema file representing table schema|{0|}}
{30001002|XXparameter|schema_file.condition|param_exact bq_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|ignore_unknown|False|13|1|RFKl|True allows the load to succeed despite the presence of columns not found in the table schema|{0|}}
{30001002|XXparameter|ignore_unknown.condition|param_exact bq_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|allow_schema_update|False|13|1|RFl|True allows the table schema to be updated as a side effect of the load|{0|}}
{30001002|XXparameter|allow_schema_update.condition|param_exact bq_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|allow_field_addition|False|13|1|FKl|True allows adding nullable fields to the table schema.|{0|}}
{30001002|XXparameter|allow_field_addition.condition|param_exact bq_util_out True param_exact allow_schema_update True|3|15|P?|@{0|}}
{30001002|XXparameter|allow_field_relaxation|False|13|1|FKl|True allows changing required fields to nullable in the table schema|{0|}}
{30001002|XXparameter|allow_field_relaxation.condition|param_exact bq_util_out True param_exact allow_schema_update True|3|15|P?|@{0|}}
{30001002|XXparameter|ss_escaped_by||3|2|FK$|[SQL statement modifier] A string (usually, one-character) defining a character escape sequence.  If non-empty, the SQL statement used for loading the data will be modified accordingly.  A given database's rules may be fairly complex -- read its documentation if you specify any value here.|{0|}}
{30001002|XXparameter|ss_escaped_by.condition|mode output param_exact supports_ss_escaped_by True|3|15|P?|@{0|}}
{30001002|XXparameter|supports_ss_escaped_by|value mysql__interface utility True value mysql__interface api False default constant False|13|13|Hv||{0|}}
{30001002|XXparameter|ss_string_for_null||3|2|FK$|[SQL statement modifier] The string representing the database NULL.  An empty string here means just that, not the database-specific default, which has to be specified explicitly.  Read the database documentation for a bulk-insert SQL statement to find the latter.|{0|}}
{30001002|XXparameter|ss_string_for_null.condition|mode output param_exact supports_ss_string_for_null True|3|15|P?|@{0|}}
{30001002|XXparameter|supports_ss_string_for_null|value vertica__interface utility True value vertica__interface api False default constant False|13|13|Hv||{0|}}
{30001002|XXparameter|ss_appendix||3|2|FK$|[SQL statement modifier] A free-form string going to the end of the SQL statement.  An arbitrary string to be appended to the end of the SQL statement used for data  loading (e.g. DIRECT).  Use this carefully to experiment with loading options:  there is no guarantee that the modified statement will work or result in a better  performance.  Read the database documentation and watch logs to understand the  possible modifications here and their operational impact.|{0|}}
{30001002|XXparameter|ss_appendix.condition|mode output param_exact supports_ss_appendix True|3|15|P?|@{0|}}
{30001002|XXparameter|supports_ss_appendix|value mysql__interface utility True value mysql__interface api False value postgresql__interface copy True value postgresql__interface api False value greenplum__interface api False value greenplum__interface copy True value greenplum__interface gpfdist True value vertica__interface utility True value vertica__interface api False default constant False|13|13|Hv||{0|}}
{30001002|XXparameter|mysql_interface|api|15|1|Fl|Unload data via api|{1|api|}}
{30001002|XXparameter|mysql_interface.condition|param_exact dbms mysql mode input|3|15|P?|@{0|}}
{30001002|XXparameter|mysql_util_in|value mysql_interface utility True value mysql_interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|mysql_util_in.condition|param_exact dbms mysql mode input|3|15|P?|@{0|}}
{30001002|XXparameter|mysql__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|mysql__interface.condition|param_exact dbms mysql mode output|3|15|P?|@{0|}}
{30001002|XXparameter|mysql_util_out|value mysql__interface utility True value mysql__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|mysql_util_out.condition|param_exact dbms mysql mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ss_duplicate_rows_handling|ignore|15|1|FKl|[SQL statement modifier] Handling of input rows that duplicate existing rows on unique key values.  With 'ignore', input rows that duplicate an existing row on a unique key value are skipped.  With 'replace', input rows replace existing rows.|{2|ignore|replace|}}
{30001002|XXparameter|ss_duplicate_rows_handling.condition|param_exact mysql_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|ss_character_set||3|2|FK$|[SQL statement modifier] Character set that MySQL uses to interpret the incoming data flow.  If not specified, determined by MySQL server 'character_set_database' system variable.|{0|}}
{30001002|XXparameter|ss_character_set.condition|param_exact mysql_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|vertica_interface|api|15|1|Fl|Unload data via api|{1|api|}}
{30001002|XXparameter|vertica_interface.condition|param_exact dbms vertica mode input|3|15|P?|@{0|}}
{30001002|XXparameter|vertica_util_in|value vertica_interface utility True value vertica_interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|vertica_util_in.condition|param_exact dbms vertica mode input|3|15|P?|@{0|}}
{30001002|XXparameter|vertica__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|vertica__interface.condition|param_exact dbms vertica mode output|3|15|P?|@{0|}}
{30001002|XXparameter|vertica_util_out|value vertica__interface utility True value vertica__interface api False default constant False|13|13|v||{0|}}
{30001002|XXparameter|vertica_util_out.condition|param_exact dbms vertica mode output|3|15|P?|@{0|}}
{30001002|XXparameter|gendml_param_names|column_delimiter fixed_size_dml field_type_preference dml_with_default_values generate_loader_datatypes ablocal_expr parent_segment_data generate_unique_key file_password file_number|3|2|H$||{0|}}
{30001002|XXparameter|m_db_layout_params|ablocal_expr|3|2|H$||{0|}}
{30001002|XXparameter|m_db_layout_params_db2uss|ablocal_expr db2uss_util_out utility|3|2|H$||{0|}}
{30001002|XXparameter|m_db_layout_params_db2uss.condition|param_exact dbms db2uss|3|15|P?|@{0|}}
{30001002|XXparameter|m_db_layout_param_names|sameas m_db_layout_params_db2uss sameas m_db_layout_params|3|13|Hv||{0|}}
{30001002|XXparameter|m_db_dump_param_names|remove_delimiters delimiter_replacement ablocal_expr|3|2|H$||{0|}}
{30001002|XXparameter|dbms||3|2|RF$|Database|{0|}}
{30001002|XXparameter|override_mpname|value dbms ims itable-ims value dbms mongo itable-mongo default dollar_substitution itable|3|13|v||{0|}}
{30001002|XXparameter|override_mpname.display_name|override-mpname|3|9|P|@{0|}}
{30001002|XXparameter|override_mpname.condition|mode input mode input|3|15|P?|@{0|}}
{30001002|XXparameter|package||8|1|FKl|Functions for deciding whether to abort or retry a transaction|{0|}}
{30001002|XXparameter|package.condition|mode output param_exact supports_package True version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|reject_threshold|Abort on first reject|15|1|Fl|When to abort if input records are rejected|{3|Abort on first reject|Never abort|Use limit/ramp|}}
{30001002|XXparameter|reject_threshold.display_name|reject-threshold|3|9|P|@{0|}}
{30001002|XXparameter|reject_threshold.condition|mode output param_exact supports_reject_threshold True version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|limit|0|1|2|F$|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|limit.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|ramp|0.0|2|2|F$|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|limit_keyword|value reject-threshold Never* 0 value reject-threshold Abort* 0 sameas limit default dollar_substitution 0|1|13|Kv|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|ramp_keyword|value reject-threshold Never* -1 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword.condition|version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|logging|False|13|1|Fl|Log internal events|{0|}}
{30001002|XXparameter|logging.condition|mode output param_exact supports_logging True version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|log|log_concat|3|13|Kv|Special log parameter|{0|}}
{30001002|XXparameter|log.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_input||15|1|Fl|Frequency of input records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_input.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_reject||15|1|Fl|Frequency of reject records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_reject.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|supports_error_group|value interface api True value mssql__interface bulkload True value ter_util True True value dbms * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|error_group||3|2|F$|Optional group name identifying a Handle Errors component to which errors can be directed.|{0|}}
{30001002|XXparameter|error_group.condition|version 2.16: mode output param_exact supports_error_group True|3|15|P?|@{0|}}
{30001002|XXparameter|log_group||3|2|F$|Optional group name of a Handle Logs component to which log output can be directed.|{0|}}
{30001002|XXparameter|log_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|keyword_map|limit_keyword limit ramp_keyword ramp|3|2|$||{0|}}
{30001002|XXparameter|keyword_map.display_name|keyword-map|3|9|P|@{0|}}
{30001002|XXparameter|_ab_insert_statement_supported|value dbms ims False value dbms adabas False value interface_out api True value dbms teradata True value dbms informix True value dbms aster True value dbms snowflake True value dbms postgresql True value dbms greenplum True value dbms redshift True value dbms mongo False value dbms hbase False value dbms * False default constant False|13|13|v||{0|}}
{30001002|XXparameter|_ab_select_statement_supported|value dbms "" True value dbms ims False value dbms adabas False value dbms mongo False value dbms hbase False value dbms * True default constant True|13|13|v||{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|_use_rich_log_format|False|13|1|Hl|False means use legacy native charset format|{0|}}
{30001002|XXparameter|num_variables|1|1|2|H$|Number of intermediate types|{0|}}
{30001002|XXparameter|num_variables.condition|param_exact supports_api_load_transforms True version 3.2.5.r24.0:|3|15|P?|@{0|}}
{30001002|XXparameter|variable1|statement_input_type statement_input "record end"|3|2|H$|Intermediate type for sql statement input|{0|}}
{30001002|XXparameter|variable1.condition|param_exact supports_api_load_transforms True version 3.2.5.r24.0:|3|15|P?|@{0|}}
{30001002|XXparameter|variable1_analysis|statement_input_type statement_input[n]|3|2|H$||{0|}}
{30001002|XXparameter|variable1_analysis.condition|param_exact supports_api_load_transforms True version 3.2.5.r24.0:|3|15|P?|@{0|}}
{30001002|XXparameter|num_types|2|1|2|H$|Number of private data types in package|{0|}}
{30001002|XXparameter|type1|error_info_t error_info "record string('', charset='x-ab-internal') component; int port_index; string('', charset='x-ab-internal') parameter; string('', charset='x-ab-internal') message; record string('', charset='x-ab-internal') code; int parent_index; record string('', charset='x-ab-internal') name; string('', charset='x-ab-internal') value; end[int] attributes; end[int] details; string('', charset='x-ab-internal') error_message; string('', charset='x-ab-internal') record_number;end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|type2|log_event_t log_event "record string('', charset='x-ab-internal') event_type; string('', charset='x-ab-internal') event_text; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|operation1|optional error::make_error(error_info, write)|3|2|H$|Function to create error record in case of error.|{0|}}
{30001002|XXparameter|operation2|optional log_event::log_error(error_info, write)|3|2|H$|Function to create log record in case of error.|{0|}}
{30001002|XXparameter|operation3|optional log_event::final_log_output()|3|2|H$|Function to create final log record.|{0|}}
{30001002|XXparameter|operation4|integer(4) out::length(in)|3|2|H$|Number of records to send to sql statements|{0|}}
{30001002|XXparameter|operation4.condition|param_exact supports_api_load_transforms True version 3.2.5.r24.0:|3|15|P?|@{0|}}
{30001002|XXparameter|operation5|statement_input::compute_statement_input(write, integer(4) index)|3|2|H$|Compute input for sql statement|{0|}}
{30001002|XXparameter|operation5.condition|param_exact supports_api_load_transforms True version 3.2.5.r24.0:|3|15|P?|@{0|}}
{30001002|XXparameter|operation4_analysis|integer(4) out_length[n]::length(write)|3|2|H$||{0|}}
{30001002|XXparameter|operation5_analysis|statement_input ? statement_input::compute_statement_input(write, integer(4) index) :COPY statement_input::compute_statement_input(write)|3|2|H$||{0|}}
{30001002|XXparameter|num_operations|5|1|2|H$|Number of operations in package|{0|}}
{30001002|XXparameter|num_callbacks|5|1|2|H$|Number of callbacks in package|{0|}}
{30001002|XXparameter|_use_rich_error_format|False|13|1|Hl|False means use legacy string newline format|{0|}}
{30001002|XXparameter|port_analysis|reject=write|3|2|H$||{0|}}
{30001002|XXparameter|Layout|$[[record kind 84 subkind 68 parts [vector]]]|9|9|RF||{0|}}
{30001002|XXparameter|eme_dataset_mapping||40|9|F|Place in the EME to create the dataset(s) corresponding to this component.|{0|}}
{30001002|XXparameter|write_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|reject_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|error_metadata|string('\\n')|7|1|Fl||{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|7|1|Fl||{0|}}
{30001002|XXparameter|mpcmodtime|1699680541|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagation|metadata type: log = "record string(\\"\|\\") node, timestamp, component, subcomponent, event_type; string(\\"\|\\\\n\\") event_text; end"
metadata type: reject = write
metadata type: error = "string('\\\\n')"|3|1|HKl|@{0|}}
}}@0|Output Table|0|0|0|0|0|0|0|@||1|100|-1|@6|@1|}}
{2010203004|XXGoport|73|0|185|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|74|0|188|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|75|0|191|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|76|0|194|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|write|0.0|@@@1936|0|}}
{2010501005|XXGpvertex|77|0|199|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Reformat.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|transform0|out :: reformat(in) = begin
  out.* :: in.*;
end|3|1|l|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject0_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|out0_metadata|$AI_DML/IODS_CONS_CSV_DNTL_CLMDTL_HX_BR1_V353S6P3_write.dml|3|2|f$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
}}@0|Reformat|2674000|224000|0|0|0|0|123|RFMT V377S0P1 V353S6P3Adaptor DS DNTL CLMDTL STG|Ab Initio Software|Built-in 1.0:|1|100|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|78|0|201|0|{@{}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|79|0|204|0|{@{}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|80|0|206|0|{@{}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|81|0|208|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|82|0|210|0|{@{}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|83|0|213|0|{Changes the record format of your data by dropping fields or by using DML expressions to add fields, combine fields, or modify the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|reformat-transform|3|1|Hl||{0|}}
{30001002|XXparameter|image__|~ab_home/bin/unitool|3|2|H$||{0|}}
{30001002|XXparameter|count|1|1|1|Fl|Number of reformat transforms|{0|}}
{30001002|XXparameter|count.is_index|true|3|9|P|@{0|}}
{30001002|XXparameter|transform0||8|1|FOl|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
{30001002|XXparameter|select||20|2|FK$|Filter expression before reformatting|{0|}}
{30001002|XXparameter|error_group||3|2|F$|Optional group name identifying a Handle Errors component to which errors can be directed.|{0|}}
{30001002|XXparameter|error_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|log_group||3|2|F$|Optional group name of a Handle Logs component to which log output can be directed.|{0|}}
{30001002|XXparameter|log_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|reject_threshold|Abort on first reject|15|1|Fl|When to abort if input records are rejected|{3|Abort on first reject|Never abort|Use limit/ramp|}}
{30001002|XXparameter|reject_threshold.display_name|reject-threshold|3|9|P|@{0|}}
{30001002|XXparameter|limit|0|1|2|F$|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|limit.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|ramp|0.0|2|2|F$|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|limit_keyword|value reject-threshold Never* 0 value reject-threshold Abort* 0 sameas limit default dollar_substitution 0|1|13|Kv|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|ramp_keyword|value reject-threshold Never* 99.0 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword.condition|version :2.12.999.m999|3|15|P?|@{0|}}
{30001002|XXparameter|ramp_keyword2|value reject-threshold Never* -1 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword2.condition|version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|keyword_map|limit_keyword limit ramp_keyword ramp ramp_keyword2 ramp|3|2|$||{0|}}
{30001002|XXparameter|keyword_map.display_name|keyword-map|3|9|P|@{0|}}
{30001002|XXparameter|output_index||8|1|FKl|Output index computation|{0|}}
{30001002|XXparameter|output_index.display_name|output-index|3|9|P|@{0|}}
{30001002|XXparameter|output_index.keyword|output-index|3|9|P|@{0|}}
{30001002|XXparameter|output_index_port|discriminator|3|2|FHK$|Output index port for debugger|{0|}}
{30001002|XXparameter|output_index_port.display_name|output-index_port|3|9|P|@{0|}}
{30001002|XXparameter|output_index_port.keyword|output-index_port|3|9|P|@{0|}}
{30001002|XXparameter|output_index_signature|int output_index_out::output_index(in)|3|2|H$|Function returning index of output port|{0|}}
{30001002|XXparameter|output_index_signature.display_name|output-index_signature|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes||8|1|FKl|Output index vector computation|{0|}}
{30001002|XXparameter|output_indexes.display_name|output-indexes|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes.keyword|output-indexes|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_port|discriminator|3|2|FHK$|Output index port for debugger|{0|}}
{30001002|XXparameter|output_indexes_port.display_name|output-indexes_port|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_port.keyword|output-indexes_port|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_signature|int[int] output_indexes_out::output_indexes(in)|3|2|H$|Function returning vector of indexes of output ports|{0|}}
{30001002|XXparameter|output_indexes_signature.display_name|output-indexes_signature|3|9|P|@{0|}}
{30001002|XXparameter|logging|False|13|1|Fl|Log internal events|{0|}}
{30001002|XXparameter|log|log_concat|3|13|Kv|Special log parameter|{0|}}
{30001002|XXparameter|log.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_input||15|1|Fl|Frequency of input records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_input.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_output||15|1|Fl|Frequency of output records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_output.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_reject||15|1|Fl|Frequency of reject records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_reject.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|num_operations|7|1|2|H$|Number of operations in package|{0|}}
{30001002|XXparameter|operation1|out::reformat(in)|3|2|H$|Reformat operation|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out*|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|legacy_xfr;reject*=in;out*=in|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis||3|2|H$||{0|}}
{30001002|XXparameter|filter_aggregate_analysis|out=filter(select)|3|2|H$||{0|}}
{30001002|XXparameter|num_types|3|1|2|H$|Number of private data types in package|{0|}}
{30001002|XXparameter|type1|error_info_t error_info "record string('', charset='x-ab-internal') component; int port_index; string('', charset='x-ab-internal') parameter; string('', charset='x-ab-internal') message; record string('', charset='x-ab-internal') code; int parent_index; record string('', charset='x-ab-internal') name; string('', charset='x-ab-internal') value; end[int] attributes; end[int] details; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|type2|log_event_t log_event "record string('', charset='x-ab-internal') event_type; string('', charset='x-ab-internal') event_text; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|type3|event_info_t event_info "record int event_type; end"|3|2|H$|Information about the kind of event that happened|{0|}}
{30001002|XXparameter|operation2|optional out::output_for_error(error_info, in)|3|2|H$|Function to create output in case of error.|{0|}}
{30001002|XXparameter|operation3|optional error::make_error(error_info, in)|3|2|H$|Function to create error record in case of error.|{0|}}
{30001002|XXparameter|operation4|optional log_event::log_error(error_info, in)|3|2|H$|Function to create log record in case of error.|{0|}}
{30001002|XXparameter|operation5|optional log_event::final_log_output()|3|2|H$|Function to create final log record.|{0|}}
{30001002|XXparameter|operation6|optional out::output_at_event(event_info)|3|2|H$|Function to allow the output of an additional record at event|{0|}}
{30001002|XXparameter|operation7|optional ::on_startup(long in_recovery)|3|2|H$|Function executed at graph startup|{0|}}
{30001002|XXparameter|num_callbacks|6|1|2|H$|Number of callbacks in package|{0|}}
{30001002|XXparameter|_use_rich_error_format|True|13|1|Hl|False means use legacy string newline format|{0|}}
{30001002|XXparameter|_use_rich_log_format|True|13|1|Hl|False means use legacy native charset format|{0|}}
{30001002|XXparameter|Layout|@9|9|RFs||{0|}}
{30001002|XXparameter|main_mp_port|count out|3|1|l||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out0_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|reject0_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|error0_metadata|$AB_HOME/include/error-info.dml|7|2|Ff$||{0|}}
{30001002|XXparameter|log_metadata|$AB_HOME/include/log-info.dml|7|2|Ff$||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out[*],reject[*],error[*],log::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1669401747|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagation|metadata type: error = remote("$AB_HOME/include/error-info.dml")
metadata type: reject = in
metadata type: log = remote("$AB_HOME/include/log-info.dml")|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|Reformat|0|0|0|0|0|0|0|@||1|100|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|84|0|215|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
{30001002|XXparameter|transform0||8|8|FO=|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
}}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|85|0|219|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|86|0|222|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|87|0|225|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|88|0|228|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|89|0|232|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Reformat.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|transform0|$AI_XFR/IODS_CONS_CSV_DNTL_CLMDTL_HX_BR1_V353S6P2.xfr|3|2|f$|@{0|}}
{30001002|XXparameter|out0_metadata|$AI_DML/IODS_CONS_CSV_DNTL_CLMDTL_HX_BR1_V353S6P2_write.dml|3|2|f$|@{0|}}
{30001002|XXparameter|transform1|$AI_XFR/IODS_CONS_CSV_DNTL_CLMDTL_HX_BR1_V353S6P3.xfr|3|2|f$|@{0|}}
{30001002|XXparameter|out1_metadata|$AI_DML/IODS_CONS_CSV_DNTL_CLMDTL_HX_BR1_V353S6P3_write.dml|3|2|f$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject0_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject1_metadata||3|8|s=|@{0|}}
}}@0|Reformat|2117000|100000|0|0|0|0|142|RFMT V353S6 Xfm Jnr|Ab Initio Software|Built-in 1.0:|1|100|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|90|0|234|0|{@{}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|91|0|237|0|{@{}@0|0|0|0|out1|0.0|count|out|1|2448|0|}}
{2010203004|XXGoport|92|0|240|0|{@{}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|93|0|242|0|{@{}@0|0|0|0|reject1|0.0|count|reject|1|1168|0|}}
{2010203004|XXGoport|94|0|244|0|{@{}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|95|0|246|0|{@{}@0|0|0|0|error1|0.0|count|error|1|1176|0|}}
{2010203004|XXGoport|96|0|248|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|97|0|250|0|{@{}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|98|0|253|0|{Changes the record format of your data by dropping fields or by using DML expressions to add fields, combine fields, or modify the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|reformat-transform|3|1|Hl||{0|}}
{30001002|XXparameter|image__|~ab_home/bin/unitool|3|2|H$||{0|}}
{30001002|XXparameter|count|2|1|1|Fl|Number of reformat transforms|{0|}}
{30001002|XXparameter|count.is_index|true|3|9|P|@{0|}}
{30001002|XXparameter|transform0||8|1|FOl|Reformat transform|{0|}}
{30001002|XXparameter|transform1||8|1|FOl|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
{30001002|XXparameter|select||20|2|FK$|Filter expression before reformatting|{0|}}
{30001002|XXparameter|error_group||3|2|F$|Optional group name identifying a Handle Errors component to which errors can be directed.|{0|}}
{30001002|XXparameter|error_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|log_group||3|2|F$|Optional group name of a Handle Logs component to which log output can be directed.|{0|}}
{30001002|XXparameter|log_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|reject_threshold|Abort on first reject|15|1|Fl|When to abort if input records are rejected|{3|Abort on first reject|Never abort|Use limit/ramp|}}
{30001002|XXparameter|reject_threshold.display_name|reject-threshold|3|9|P|@{0|}}
{30001002|XXparameter|limit|0|1|2|F$|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|limit.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|ramp|0.0|2|2|F$|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|limit_keyword|value reject-threshold Never* 0 value reject-threshold Abort* 0 sameas limit default dollar_substitution 0|1|13|Kv|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|ramp_keyword|value reject-threshold Never* 99.0 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword.condition|version :2.12.999.m999|3|15|P?|@{0|}}
{30001002|XXparameter|ramp_keyword2|value reject-threshold Never* -1 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword2.condition|version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|keyword_map|limit_keyword limit ramp_keyword ramp ramp_keyword2 ramp|3|2|$||{0|}}
{30001002|XXparameter|keyword_map.display_name|keyword-map|3|9|P|@{0|}}
{30001002|XXparameter|output_index||8|1|FKl|Output index computation|{0|}}
{30001002|XXparameter|output_index.display_name|output-index|3|9|P|@{0|}}
{30001002|XXparameter|output_index.keyword|output-index|3|9|P|@{0|}}
{30001002|XXparameter|output_index_port|discriminator|3|2|FHK$|Output index port for debugger|{0|}}
{30001002|XXparameter|output_index_port.display_name|output-index_port|3|9|P|@{0|}}
{30001002|XXparameter|output_index_port.keyword|output-index_port|3|9|P|@{0|}}
{30001002|XXparameter|output_index_signature|int output_index_out::output_index(in)|3|2|H$|Function returning index of output port|{0|}}
{30001002|XXparameter|output_index_signature.display_name|output-index_signature|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes||8|1|FKl|Output index vector computation|{0|}}
{30001002|XXparameter|output_indexes.display_name|output-indexes|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes.keyword|output-indexes|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_port|discriminator|3|2|FHK$|Output index port for debugger|{0|}}
{30001002|XXparameter|output_indexes_port.display_name|output-indexes_port|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_port.keyword|output-indexes_port|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_signature|int[int] output_indexes_out::output_indexes(in)|3|2|H$|Function returning vector of indexes of output ports|{0|}}
{30001002|XXparameter|output_indexes_signature.display_name|output-indexes_signature|3|9|P|@{0|}}
{30001002|XXparameter|logging|False|13|1|Fl|Log internal events|{0|}}
{30001002|XXparameter|log|log_concat|3|13|Kv|Special log parameter|{0|}}
{30001002|XXparameter|log.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_input||15|1|Fl|Frequency of input records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_input.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_output||15|1|Fl|Frequency of output records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_output.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_reject||15|1|Fl|Frequency of reject records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_reject.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|num_operations|7|1|2|H$|Number of operations in package|{0|}}
{30001002|XXparameter|operation1|out::reformat(in)|3|2|H$|Reformat operation|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out*|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|legacy_xfr;reject*=in;out*=in|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis||3|2|H$||{0|}}
{30001002|XXparameter|filter_aggregate_analysis|out=filter(select)|3|2|H$||{0|}}
{30001002|XXparameter|num_types|3|1|2|H$|Number of private data types in package|{0|}}
{30001002|XXparameter|type1|error_info_t error_info "record string('', charset='x-ab-internal') component; int port_index; string('', charset='x-ab-internal') parameter; string('', charset='x-ab-internal') message; record string('', charset='x-ab-internal') code; int parent_index; record string('', charset='x-ab-internal') name; string('', charset='x-ab-internal') value; end[int] attributes; end[int] details; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|type2|log_event_t log_event "record string('', charset='x-ab-internal') event_type; string('', charset='x-ab-internal') event_text; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|type3|event_info_t event_info "record int event_type; end"|3|2|H$|Information about the kind of event that happened|{0|}}
{30001002|XXparameter|operation2|optional out::output_for_error(error_info, in)|3|2|H$|Function to create output in case of error.|{0|}}
{30001002|XXparameter|operation3|optional error::make_error(error_info, in)|3|2|H$|Function to create error record in case of error.|{0|}}
{30001002|XXparameter|operation4|optional log_event::log_error(error_info, in)|3|2|H$|Function to create log record in case of error.|{0|}}
{30001002|XXparameter|operation5|optional log_event::final_log_output()|3|2|H$|Function to create final log record.|{0|}}
{30001002|XXparameter|operation6|optional out::output_at_event(event_info)|3|2|H$|Function to allow the output of an additional record at event|{0|}}
{30001002|XXparameter|operation7|optional ::on_startup(long in_recovery)|3|2|H$|Function executed at graph startup|{0|}}
{30001002|XXparameter|num_callbacks|6|1|2|H$|Number of callbacks in package|{0|}}
{30001002|XXparameter|_use_rich_error_format|True|13|1|Hl|False means use legacy string newline format|{0|}}
{30001002|XXparameter|_use_rich_log_format|True|13|1|Hl|False means use legacy native charset format|{0|}}
{30001002|XXparameter|Layout|@9|9|RFs||{0|}}
{30001002|XXparameter|main_mp_port|count out|3|1|l||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out0_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out1_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|reject0_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|reject1_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|error0_metadata|$AB_HOME/include/error-info.dml|7|2|Ff$||{0|}}
{30001002|XXparameter|error1_metadata|$AB_HOME/include/error-info.dml|7|2|Ff$||{0|}}
{30001002|XXparameter|log_metadata|$AB_HOME/include/log-info.dml|7|2|Ff$||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out[*],reject[*],error[*],log::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1669401747|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagation|metadata type: error = remote("$AB_HOME/include/error-info.dml")
metadata type: reject = in
metadata type: log = remote("$AB_HOME/include/log-info.dml")|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
{30001002|XXparameter|transform1.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform1.index_value|1|3|9|P|@{0|}}
{30001002|XXparameter|transform1.index_name|transform|3|9|P|@{0|}}
}}@0|Reformat|0|0|0|0|0|0|0|@||1|100|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|99|0|255|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
{30001002|XXparameter|transform0||8|8|FO=|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
}}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|100|0|259|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
{30001002|XXparameter|transform1||8|8|FO=|Reformat transform|{0|}}
{30001002|XXparameter|transform1.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform1.index_value|1|3|9|P|@{0|}}
{30001002|XXparameter|transform1.index_name|transform|3|9|P|@{0|}}
}}@0|0|0|0|out1|0.0|count|out|1|2448|0|}}
{2010203004|XXGoport|101|0|263|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|102|0|266|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject1|0.0|count|reject|1|1168|0|}}
{2010203004|XXGoport|103|0|269|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|104|0|272|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error1|0.0|count|error|1|1176|0|}}
{2010203004|XXGoport|105|0|275|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|106|0|278|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|107|0|287|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Partitioning/Partition_by_Key.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|key|\{AK_UCK_ID; AK_UCK_ID_PREFIX_CD; AK_UCK_ID_SEGMENT_NO\}|3|2|$|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|out_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
}}@0|Partition by Key|1098000|143000|0|0|0|0|151|Partition by Key-3|Ab Initio Software|Built-in 1.0:|1|100|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|108|0|289|0|{@{}@0|0|0|0|out|0.0|@@@2322|0|}}
{2010202004|XXGiport|109|0|292|0|{@{}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|110|0|295|0|{Distributes data records to its output flow partitions according to key values.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|hash-partition|3|1|Hl||{0|}}
{30001002|XXparameter|image__|~ab_home/bin/unitool|3|2|H$||{0|}}
{30001002|XXparameter|key||19|2|RFO$|Field to partition on|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|out=in|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis||3|2|H$||{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1690331344|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagate_through|metadata type: out = in
metadata type: in = out|3|1|FHKl|@{0|}}
}}@0|Partition by Key|0|0|0|0|0|0|0|@||1|100|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|111|0|297|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2322|0|}}
{2010202004|XXGiport|112|0|300|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|113|0|307|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Reformat.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|transform0|out :: reformat(in) = begin
  out.* :: in.*;
end|3|1|l|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|out0_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject0_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
}}@0|Reformat|2679000|56000|0|0|0|0|121|RFMT V353S5P2 V353S6P2Adaptor DS CONS CSV DENTAL CLMDTL HX|Ab Initio Software|Built-in 1.0:|1|100|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|114|0|309|0|{@{}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|115|0|312|0|{@{}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|116|0|314|0|{@{}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|117|0|316|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|118|0|318|0|{@{}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|119|0|321|0|{Changes the record format of your data by dropping fields or by using DML expressions to add fields, combine fields, or modify the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|reformat-transform|3|1|Hl||{0|}}
{30001002|XXparameter|image__|~ab_home/bin/unitool|3|2|H$||{0|}}
{30001002|XXparameter|count|1|1|1|Fl|Number of reformat transforms|{0|}}
{30001002|XXparameter|count.is_index|true|3|9|P|@{0|}}
{30001002|XXparameter|transform0||8|1|FOl|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
{30001002|XXparameter|select||20|2|FK$|Filter expression before reformatting|{0|}}
{30001002|XXparameter|error_group||3|2|F$|Optional group name identifying a Handle Errors component to which errors can be directed.|{0|}}
{30001002|XXparameter|error_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|log_group||3|2|F$|Optional group name of a Handle Logs component to which log output can be directed.|{0|}}
{30001002|XXparameter|log_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|reject_threshold|Abort on first reject|15|1|Fl|When to abort if input records are rejected|{3|Abort on first reject|Never abort|Use limit/ramp|}}
{30001002|XXparameter|reject_threshold.display_name|reject-threshold|3|9|P|@{0|}}
{30001002|XXparameter|limit|0|1|2|F$|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|limit.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|ramp|0.0|2|2|F$|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|limit_keyword|value reject-threshold Never* 0 value reject-threshold Abort* 0 sameas limit default dollar_substitution 0|1|13|Kv|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|ramp_keyword|value reject-threshold Never* 99.0 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword.condition|version :2.12.999.m999|3|15|P?|@{0|}}
{30001002|XXparameter|ramp_keyword2|value reject-threshold Never* -1 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword2.condition|version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|keyword_map|limit_keyword limit ramp_keyword ramp ramp_keyword2 ramp|3|2|$||{0|}}
{30001002|XXparameter|keyword_map.display_name|keyword-map|3|9|P|@{0|}}
{30001002|XXparameter|output_index||8|1|FKl|Output index computation|{0|}}
{30001002|XXparameter|output_index.display_name|output-index|3|9|P|@{0|}}
{30001002|XXparameter|output_index.keyword|output-index|3|9|P|@{0|}}
{30001002|XXparameter|output_index_port|discriminator|3|2|FHK$|Output index port for debugger|{0|}}
{30001002|XXparameter|output_index_port.display_name|output-index_port|3|9|P|@{0|}}
{30001002|XXparameter|output_index_port.keyword|output-index_port|3|9|P|@{0|}}
{30001002|XXparameter|output_index_signature|int output_index_out::output_index(in)|3|2|H$|Function returning index of output port|{0|}}
{30001002|XXparameter|output_index_signature.display_name|output-index_signature|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes||8|1|FKl|Output index vector computation|{0|}}
{30001002|XXparameter|output_indexes.display_name|output-indexes|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes.keyword|output-indexes|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_port|discriminator|3|2|FHK$|Output index port for debugger|{0|}}
{30001002|XXparameter|output_indexes_port.display_name|output-indexes_port|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_port.keyword|output-indexes_port|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_signature|int[int] output_indexes_out::output_indexes(in)|3|2|H$|Function returning vector of indexes of output ports|{0|}}
{30001002|XXparameter|output_indexes_signature.display_name|output-indexes_signature|3|9|P|@{0|}}
{30001002|XXparameter|logging|False|13|1|Fl|Log internal events|{0|}}
{30001002|XXparameter|log|log_concat|3|13|Kv|Special log parameter|{0|}}
{30001002|XXparameter|log.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_input||15|1|Fl|Frequency of input records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_input.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_output||15|1|Fl|Frequency of output records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_output.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_reject||15|1|Fl|Frequency of reject records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_reject.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|num_operations|7|1|2|H$|Number of operations in package|{0|}}
{30001002|XXparameter|operation1|out::reformat(in)|3|2|H$|Reformat operation|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out*|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|legacy_xfr;reject*=in;out*=in|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis||3|2|H$||{0|}}
{30001002|XXparameter|filter_aggregate_analysis|out=filter(select)|3|2|H$||{0|}}
{30001002|XXparameter|num_types|3|1|2|H$|Number of private data types in package|{0|}}
{30001002|XXparameter|type1|error_info_t error_info "record string('', charset='x-ab-internal') component; int port_index; string('', charset='x-ab-internal') parameter; string('', charset='x-ab-internal') message; record string('', charset='x-ab-internal') code; int parent_index; record string('', charset='x-ab-internal') name; string('', charset='x-ab-internal') value; end[int] attributes; end[int] details; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|type2|log_event_t log_event "record string('', charset='x-ab-internal') event_type; string('', charset='x-ab-internal') event_text; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|type3|event_info_t event_info "record int event_type; end"|3|2|H$|Information about the kind of event that happened|{0|}}
{30001002|XXparameter|operation2|optional out::output_for_error(error_info, in)|3|2|H$|Function to create output in case of error.|{0|}}
{30001002|XXparameter|operation3|optional error::make_error(error_info, in)|3|2|H$|Function to create error record in case of error.|{0|}}
{30001002|XXparameter|operation4|optional log_event::log_error(error_info, in)|3|2|H$|Function to create log record in case of error.|{0|}}
{30001002|XXparameter|operation5|optional log_event::final_log_output()|3|2|H$|Function to create final log record.|{0|}}
{30001002|XXparameter|operation6|optional out::output_at_event(event_info)|3|2|H$|Function to allow the output of an additional record at event|{0|}}
{30001002|XXparameter|operation7|optional ::on_startup(long in_recovery)|3|2|H$|Function executed at graph startup|{0|}}
{30001002|XXparameter|num_callbacks|6|1|2|H$|Number of callbacks in package|{0|}}
{30001002|XXparameter|_use_rich_error_format|True|13|1|Hl|False means use legacy string newline format|{0|}}
{30001002|XXparameter|_use_rich_log_format|True|13|1|Hl|False means use legacy native charset format|{0|}}
{30001002|XXparameter|Layout|@9|9|RFs||{0|}}
{30001002|XXparameter|main_mp_port|count out|3|1|l||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out0_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|reject0_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|error0_metadata|$AB_HOME/include/error-info.dml|7|2|Ff$||{0|}}
{30001002|XXparameter|log_metadata|$AB_HOME/include/log-info.dml|7|2|Ff$||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out[*],reject[*],error[*],log::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1669401747|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagation|metadata type: error = remote("$AB_HOME/include/error-info.dml")
metadata type: reject = in
metadata type: log = remote("$AB_HOME/include/log-info.dml")|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|Reformat|0|0|0|0|0|0|0|@||1|100|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|120|0|323|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
{30001002|XXparameter|transform0||8|8|FO=|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
}}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|121|0|327|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|122|0|330|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|123|0|333|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|124|0|336|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|125|0|345|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Sort/Sort.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|key|\{AK_UCK_ID; AK_UCK_ID_PREFIX_CD; AK_UCK_ID_SEGMENT_NO; AK_SUBMT_SVC_LN_NO\}|3|2|$|@{0|}}
{30001002|XXparameter|max_core|$AI_SORT_MAX_CORE|3|2|$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|out_metadata||3|8|s=|@{0|}}
}}@0|Sort|1290000|90000|0|0|0|0|143|SORT V353S0P3 S Rmv Dup keycols|Ab Initio Software|Built-in 1.0:|1|100|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|126|0|347|0|{@{}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010202004|XXGiport|127|0|350|0|{@{}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|128|0|353|0|{Orders your data according to a key specifier.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|local-sort|3|1|Hl||{0|}}
{30001002|XXparameter|image__|~ab_home/bin/unitool|3|2|H$||{0|}}
{30001002|XXparameter|key||19|2|RFO$|Field to sort on|{0|}}
{30001002|XXparameter|max_core|100663296|1|2|FK$|maximum memory usage (before spilling to disk) in bytes|{0|}}
{30001002|XXparameter|max_core.display_name|max-core|3|9|P|@{0|}}
{30001002|XXparameter|max_core.keyword|max-core|3|9|P|@{0|}}
{30001002|XXparameter|max_core.update_clauses|retain_old_default|3|9|P|@{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|out=in|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis||3|2|H$||{0|}}
{30001002|XXparameter|Layout|@9|9|RFs||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1669401747|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagate_through|metadata type: out = in
metadata type: in = out|3|1|FHKl|@{0|}}
}}@0|Sort|0|0|0|0|0|0|0|@||1|100|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|129|0|355|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010202004|XXGiport|130|0|358|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010110002|XXGconstant|131|0|366|0|{@{}@{1|6214163|}}}
{2010002001|XXGdirectory_object_object|0|1|2|0|{XXGgraph|}0|1|}
{2010704001|XXGgraph_graphinfo_graphinfo|1|0|2|0|{}1|2|}
{2010706001|XXGgraphinfo_runsettings_runsettings|2|0|4|0|{}2|3|}
{2010109001|XXGobject_property_value|3|0|6|0|{0|legend_face|92|}2|4|}
{2010605001|XXGgraph_legend_legend|4|0|8|0|{0|}1|5|}
{2010241001|XXGlegend_face_face|5|0|10|0|{}5|6|}
{2010605001|XXGgraph_legend_legend|6|0|12|0|{1|}1|7|}
{2010241001|XXGlegend_face_face|7|0|14|0|{}7|8|}
{2010604001|XXGgraph_flow_flow|8|0|16|0|{DEDU_V353S0_Rmv_Dup_keycols-1_to_RFMT_V353S6_Xfm_Jnr-1|}1|9|}
{2010604001|XXGgraph_flow_flow|9|0|18|0|{Flow_1|}1|10|}
{2010604001|XXGgraph_flow_flow|10|0|20|0|{Flow_2|}1|11|}
{2010604001|XXGgraph_flow_flow|11|0|22|0|{Flow_3|}1|12|}
{2010604001|XXGgraph_flow_flow|12|0|24|0|{Flow_4|}1|13|}
{2010604001|XXGgraph_flow_flow|13|0|26|0|{Flow_5|}1|14|}
{2010604001|XXGgraph_flow_flow|14|0|28|0|{RFMT_V353S5P2_V353S6P2Adaptor_DS_CONS_CSV_DENTAL_CLMDTL_HX0_to_V353S5_DS_CONS_CSV_DENTAL_CLMDTL_HX-1|}1|15|}
{2010604001|XXGgraph_flow_flow|15|0|30|0|{RFMT_V353S6_Xfm_Jnr0_to_RFMT_V353S5P2_V353S6P2Adaptor_DS_CONS_CSV_DENTAL_CLMDTL_HX-1|}1|16|}
{2010604001|XXGgraph_flow_flow|16|0|32|0|{RFMT_V353S6_Xfm_Jnr1_to_RFMT_V377S0P1_V353S6P3Adaptor_DS_DNTL_CLMDTL_STG-1|}1|17|}
{2010604001|XXGgraph_flow_flow|17|0|34|0|{SORT_V353S0P3_S_Rmv_Dup_keycols-1_to_DEDU_V353S0_Rmv_Dup_keycols-1|}1|18|}
{2010601001|XXGgraph_vertex_vertex|18|0|36|0|{DEDU_V353S0_Rmv_Dup_keycols|}1|19|}
{2010212001|XXGvertex_oport_oport|19|0|38|0|{0|out|}19|20|}
{2010213001|XXGoport_dst_flow|20|0|40|0|{0|}20|9|}
{2010212001|XXGvertex_oport_oport|21|0|41|0|{1|dup|}19|21|}
{2010212001|XXGvertex_oport_oport|22|0|43|0|{2|reject|}19|22|}
{2010212001|XXGvertex_oport_oport|23|0|45|0|{3|error|}19|23|}
{2010212001|XXGvertex_oport_oport|24|0|47|0|{4|log|}19|24|}
{2010211001|XXGvertex_iport_iport|25|0|49|0|{0|in|}19|25|}
{2010214001|XXGiport_src_flow|26|0|51|0|{0|}25|18|}
{2010007001|XXGobject_proto_object|27|0|52|0|{}19|26|}
{2010212001|XXGvertex_oport_oport|28|0|54|0|{0|out|}26|27|}
{2010006001|XXGobject_psameas_object|29|0|56|0|{metadata|out_metadata|1|0|}27|26|}
{2010212001|XXGvertex_oport_oport|30|0|57|0|{1|dup|}26|28|}
{2010006001|XXGobject_psameas_object|31|0|59|0|{metadata|dup_metadata|1|0|}28|26|}
{2010212001|XXGvertex_oport_oport|32|0|60|0|{2|reject|}26|29|}
{2010006001|XXGobject_psameas_object|33|0|62|0|{metadata|reject_metadata|1|0|}29|26|}
{2010212001|XXGvertex_oport_oport|34|0|63|0|{3|error|}26|30|}
{2010006001|XXGobject_psameas_object|35|0|65|0|{metadata|error_metadata|1|0|}30|26|}
{2010212001|XXGvertex_oport_oport|36|0|66|0|{4|log|}26|31|}
{2010006001|XXGobject_psameas_object|37|0|68|0|{metadata|log_metadata|1|0|}31|26|}
{2010211001|XXGvertex_iport_iport|38|0|69|0|{0|in|}26|32|}
{2010006001|XXGobject_psameas_object|39|0|71|0|{metadata|in_metadata|1|0|}32|26|}
{2010006001|XXGobject_psameas_object|40|0|72|0|{Layout|Layout|0|0|}19|33|}
{2010211001|XXGvertex_iport_iport|41|0|74|0|{0|write|}33|34|}
{2010214001|XXGiport_src_flow|42|0|76|0|{0|}34|15|}
{2010007001|XXGobject_proto_object|43|0|77|0|{}33|35|}
{2010211001|XXGvertex_iport_iport|44|0|79|0|{0|write|}35|36|}
{2010006001|XXGobject_psameas_object|45|0|81|0|{metadata|write_metadata|1|0|}36|35|}
{2010006001|XXGobject_psameas_object|46|0|82|0|{dup_metadata|out0_metadata|0|0|}19|37|}
{2010212001|XXGvertex_oport_oport|47|0|84|0|{0|out0|}37|38|}
{2010213001|XXGoport_dst_flow|48|0|86|0|{0|}38|11|}
{2010212001|XXGvertex_oport_oport|49|0|87|0|{1|reject0|}37|39|}
{2010212001|XXGvertex_oport_oport|50|0|89|0|{2|error0|}37|40|}
{2010212001|XXGvertex_oport_oport|51|0|91|0|{3|log|}37|41|}
{2010211001|XXGvertex_iport_iport|52|0|93|0|{0|in|}37|42|}
{2010214001|XXGiport_src_flow|53|0|95|0|{0|}42|13|}
{2010007001|XXGobject_proto_object|54|0|96|0|{}37|43|}
{2010212001|XXGvertex_oport_oport|55|0|98|0|{0|out0|}43|44|}
{2010006001|XXGobject_psameas_object|56|0|100|0|{metadata|out0_metadata|1|0|}44|43|}
{2010006001|XXGobject_psameas_object|57|0|101|0|{transform0|transform0|1|0|}44|43|}
{2010212001|XXGvertex_oport_oport|58|0|102|0|{1|reject0|}43|45|}
{2010006001|XXGobject_psameas_object|59|0|104|0|{metadata|reject0_metadata|1|0|}45|43|}
{2010212001|XXGvertex_oport_oport|60|0|105|0|{2|error0|}43|46|}
{2010006001|XXGobject_psameas_object|61|0|107|0|{metadata|error0_metadata|1|0|}46|43|}
{2010212001|XXGvertex_oport_oport|62|0|108|0|{3|log|}43|47|}
{2010006001|XXGobject_psameas_object|63|0|110|0|{metadata|log_metadata|1|0|}47|43|}
{2010211001|XXGvertex_iport_iport|64|0|111|0|{0|in|}43|48|}
{2010006001|XXGobject_psameas_object|65|0|113|0|{metadata|in_metadata|1|0|}48|43|}
{2010006001|XXGobject_psameas_object|66|0|114|0|{Layout|Layout|0|0|}37|49|}
{2010212001|XXGvertex_oport_oport|67|0|116|0|{0|read|}49|50|}
{2010213001|XXGoport_dst_flow|68|0|118|0|{0|}50|12|}
{2010212001|XXGvertex_oport_oport|69|0|119|0|{1|log|}49|51|}
{2010007001|XXGobject_proto_object|70|0|121|0|{}49|52|}
{2010212001|XXGvertex_oport_oport|71|0|123|0|{0|read|}52|53|}
{2010006001|XXGobject_psameas_object|72|0|125|0|{metadata|read_metadata|1|0|}53|52|}
{2010212001|XXGvertex_oport_oport|73|0|126|0|{1|log|}52|54|}
{2010006001|XXGobject_psameas_object|74|0|128|0|{metadata|log_metadata|1|0|}54|52|}
{2010006001|XXGobject_psameas_object|75|0|129|0|{Layout|table_spec|0|1|}52|52|}
{2010006001|XXGobject_psameas_object|76|0|130|0|{in_metadata|read_metadata|0|0|}37|49|}
{2010006001|XXGobject_psameas_object|77|0|131|0|{reject0_metadata|read_metadata|0|0|}37|49|}
{2010006001|XXGobject_psameas_object|78|0|132|0|{in_metadata|out0_metadata|0|0|}19|37|}
{2010006001|XXGobject_psameas_object|79|0|133|0|{out_metadata|out0_metadata|0|0|}19|37|}
{2010006001|XXGobject_psameas_object|80|0|134|0|{reject_metadata|out0_metadata|0|0|}19|37|}
{2010601001|XXGgraph_vertex_vertex|81|0|135|0|{FD_RFMT_2|}1|55|}
{2010212001|XXGvertex_oport_oport|82|0|137|0|{0|out0|}55|56|}
{2010213001|XXGoport_dst_flow|83|0|139|0|{0|}56|13|}
{2010212001|XXGvertex_oport_oport|84|0|140|0|{1|reject0|}55|57|}
{2010212001|XXGvertex_oport_oport|85|0|142|0|{2|error0|}55|58|}
{2010212001|XXGvertex_oport_oport|86|0|144|0|{3|log|}55|59|}
{2010211001|XXGvertex_iport_iport|87|0|146|0|{0|in|}55|60|}
{2010214001|XXGiport_src_flow|88|0|148|0|{0|}60|12|}
{2010007001|XXGobject_proto_object|89|0|149|0|{}55|61|}
{2010212001|XXGvertex_oport_oport|90|0|151|0|{0|out0|}61|62|}
{2010006001|XXGobject_psameas_object|91|0|153|0|{metadata|out0_metadata|1|0|}62|61|}
{2010006001|XXGobject_psameas_object|92|0|154|0|{transform0|transform0|1|0|}62|61|}
{2010212001|XXGvertex_oport_oport|93|0|155|0|{1|reject0|}61|63|}
{2010006001|XXGobject_psameas_object|94|0|157|0|{metadata|reject0_metadata|1|0|}63|61|}
{2010212001|XXGvertex_oport_oport|95|0|158|0|{2|error0|}61|64|}
{2010006001|XXGobject_psameas_object|96|0|160|0|{metadata|error0_metadata|1|0|}64|61|}
{2010212001|XXGvertex_oport_oport|97|0|161|0|{3|log|}61|65|}
{2010006001|XXGobject_psameas_object|98|0|163|0|{metadata|log_metadata|1|0|}65|61|}
{2010211001|XXGvertex_iport_iport|99|0|164|0|{0|in|}61|66|}
{2010006001|XXGobject_psameas_object|100|0|166|0|{metadata|in_metadata|1|0|}66|61|}
{2010006001|XXGobject_psameas_object|101|0|167|0|{Layout|Layout|0|0|}55|49|}
{2010006001|XXGobject_psameas_object|102|0|168|0|{in_metadata|read_metadata|0|0|}55|49|}
{2010006001|XXGobject_psameas_object|103|0|169|0|{out0_metadata|read_metadata|0|0|}55|49|}
{2010006001|XXGobject_psameas_object|104|0|170|0|{reject0_metadata|read_metadata|0|0|}55|49|}
{2010601001|XXGgraph_vertex_vertex|105|0|171|0|{Output_Table|}1|67|}
{2010212001|XXGvertex_oport_oport|106|0|173|0|{0|reject|}67|68|}
{2010212001|XXGvertex_oport_oport|107|0|175|0|{1|error|}67|69|}
{2010212001|XXGvertex_oport_oport|108|0|177|0|{2|log|}67|70|}
{2010211001|XXGvertex_iport_iport|109|0|179|0|{0|write|}67|71|}
{2010214001|XXGiport_src_flow|110|0|181|0|{0|}71|10|}
{2010007001|XXGobject_proto_object|111|0|182|0|{}67|72|}
{2010212001|XXGvertex_oport_oport|112|0|184|0|{0|reject|}72|73|}
{2010006001|XXGobject_psameas_object|113|0|186|0|{metadata|reject_metadata|1|0|}73|72|}
{2010212001|XXGvertex_oport_oport|114|0|187|0|{1|error|}72|74|}
{2010006001|XXGobject_psameas_object|115|0|189|0|{metadata|error_metadata|1|0|}74|72|}
{2010212001|XXGvertex_oport_oport|116|0|190|0|{2|log|}72|75|}
{2010006001|XXGobject_psameas_object|117|0|192|0|{metadata|log_metadata|1|0|}75|72|}
{2010211001|XXGvertex_iport_iport|118|0|193|0|{0|write|}72|76|}
{2010006001|XXGobject_psameas_object|119|0|195|0|{metadata|write_metadata|1|0|}76|72|}
{2010006001|XXGobject_psameas_object|120|0|196|0|{Layout|table_spec|0|1|}72|72|}
{2010006001|XXGobject_psameas_object|121|0|197|0|{Layout|Layout|0|0|}67|33|}
{2010006001|XXGobject_psameas_object|122|0|198|0|{reject_metadata|out0_metadata|0|0|}67|77|}
{2010212001|XXGvertex_oport_oport|123|0|200|0|{0|out0|}77|78|}
{2010213001|XXGoport_dst_flow|124|0|202|0|{0|}78|10|}
{2010212001|XXGvertex_oport_oport|125|0|203|0|{1|reject0|}77|79|}
{2010212001|XXGvertex_oport_oport|126|0|205|0|{2|error0|}77|80|}
{2010212001|XXGvertex_oport_oport|127|0|207|0|{3|log|}77|81|}
{2010211001|XXGvertex_iport_iport|128|0|209|0|{0|in|}77|82|}
{2010214001|XXGiport_src_flow|129|0|211|0|{0|}82|17|}
{2010007001|XXGobject_proto_object|130|0|212|0|{}77|83|}
{2010212001|XXGvertex_oport_oport|131|0|214|0|{0|out0|}83|84|}
{2010006001|XXGobject_psameas_object|132|0|216|0|{metadata|out0_metadata|1|0|}84|83|}
{2010006001|XXGobject_psameas_object|133|0|217|0|{transform0|transform0|1|0|}84|83|}
{2010212001|XXGvertex_oport_oport|134|0|218|0|{1|reject0|}83|85|}
{2010006001|XXGobject_psameas_object|135|0|220|0|{metadata|reject0_metadata|1|0|}85|83|}
{2010212001|XXGvertex_oport_oport|136|0|221|0|{2|error0|}83|86|}
{2010006001|XXGobject_psameas_object|137|0|223|0|{metadata|error0_metadata|1|0|}86|83|}
{2010212001|XXGvertex_oport_oport|138|0|224|0|{3|log|}83|87|}
{2010006001|XXGobject_psameas_object|139|0|226|0|{metadata|log_metadata|1|0|}87|83|}
{2010211001|XXGvertex_iport_iport|140|0|227|0|{0|in|}83|88|}
{2010006001|XXGobject_psameas_object|141|0|229|0|{metadata|in_metadata|1|0|}88|83|}
{2010006001|XXGobject_psameas_object|142|0|230|0|{Layout|Layout|0|0|}77|33|}
{2010006001|XXGobject_psameas_object|143|0|231|0|{in_metadata|out1_metadata|0|0|}77|89|}
{2010212001|XXGvertex_oport_oport|144|0|233|0|{0|out0|}89|90|}
{2010213001|XXGoport_dst_flow|145|0|235|0|{0|}90|16|}
{2010212001|XXGvertex_oport_oport|146|0|236|0|{1|out1|}89|91|}
{2010213001|XXGoport_dst_flow|147|0|238|0|{0|}91|17|}
{2010212001|XXGvertex_oport_oport|148|0|239|0|{2|reject0|}89|92|}
{2010212001|XXGvertex_oport_oport|149|0|241|0|{3|reject1|}89|93|}
{2010212001|XXGvertex_oport_oport|150|0|243|0|{4|error0|}89|94|}
{2010212001|XXGvertex_oport_oport|151|0|245|0|{5|error1|}89|95|}
{2010212001|XXGvertex_oport_oport|152|0|247|0|{6|log|}89|96|}
{2010211001|XXGvertex_iport_iport|153|0|249|0|{0|in|}89|97|}
{2010214001|XXGiport_src_flow|154|0|251|0|{0|}97|9|}
{2010007001|XXGobject_proto_object|155|0|252|0|{}89|98|}
{2010212001|XXGvertex_oport_oport|156|0|254|0|{0|out0|}98|99|}
{2010006001|XXGobject_psameas_object|157|0|256|0|{metadata|out0_metadata|1|0|}99|98|}
{2010006001|XXGobject_psameas_object|158|0|257|0|{transform0|transform0|1|0|}99|98|}
{2010212001|XXGvertex_oport_oport|159|0|258|0|{1|out1|}98|100|}
{2010006001|XXGobject_psameas_object|160|0|260|0|{metadata|out1_metadata|1|0|}100|98|}
{2010006001|XXGobject_psameas_object|161|0|261|0|{transform1|transform1|1|0|}100|98|}
{2010212001|XXGvertex_oport_oport|162|0|262|0|{2|reject0|}98|101|}
{2010006001|XXGobject_psameas_object|163|0|264|0|{metadata|reject0_metadata|1|0|}101|98|}
{2010212001|XXGvertex_oport_oport|164|0|265|0|{3|reject1|}98|102|}
{2010006001|XXGobject_psameas_object|165|0|267|0|{metadata|reject1_metadata|1|0|}102|98|}
{2010212001|XXGvertex_oport_oport|166|0|268|0|{4|error0|}98|103|}
{2010006001|XXGobject_psameas_object|167|0|270|0|{metadata|error0_metadata|1|0|}103|98|}
{2010212001|XXGvertex_oport_oport|168|0|271|0|{5|error1|}98|104|}
{2010006001|XXGobject_psameas_object|169|0|273|0|{metadata|error1_metadata|1|0|}104|98|}
{2010212001|XXGvertex_oport_oport|170|0|274|0|{6|log|}98|105|}
{2010006001|XXGobject_psameas_object|171|0|276|0|{metadata|log_metadata|1|0|}105|98|}
{2010211001|XXGvertex_iport_iport|172|0|277|0|{0|in|}98|106|}
{2010006001|XXGobject_psameas_object|173|0|279|0|{metadata|in_metadata|1|0|}106|98|}
{2010006001|XXGobject_psameas_object|174|0|280|0|{Layout|Layout|0|0|}89|33|}
{2010006001|XXGobject_psameas_object|175|0|281|0|{in_metadata|out0_metadata|0|0|}89|37|}
{2010006001|XXGobject_psameas_object|176|0|282|0|{reject0_metadata|out0_metadata|0|0|}89|37|}
{2010006001|XXGobject_psameas_object|177|0|283|0|{reject1_metadata|out0_metadata|0|0|}89|37|}
{2010006001|XXGobject_psameas_object|178|0|284|0|{reject0_metadata|out1_metadata|0|0|}77|89|}
{2010006001|XXGobject_psameas_object|179|0|285|0|{write_metadata|out0_metadata|0|0|}67|77|}
{2010601001|XXGgraph_vertex_vertex|180|0|286|0|{Partition_by_Key_3|}1|107|}
{2010212001|XXGvertex_oport_oport|181|0|288|0|{0|out|}107|108|}
{2010213001|XXGoport_dst_flow|182|0|290|0|{0|}108|14|}
{2010211001|XXGvertex_iport_iport|183|0|291|0|{0|in|}107|109|}
{2010214001|XXGiport_src_flow|184|0|293|0|{0|}109|11|}
{2010007001|XXGobject_proto_object|185|0|294|0|{}107|110|}
{2010212001|XXGvertex_oport_oport|186|0|296|0|{0|out|}110|111|}
{2010006001|XXGobject_psameas_object|187|0|298|0|{metadata|out_metadata|1|0|}111|110|}
{2010211001|XXGvertex_iport_iport|188|0|299|0|{0|in|}110|112|}
{2010006001|XXGobject_psameas_object|189|0|301|0|{metadata|in_metadata|1|0|}112|110|}
{2010006001|XXGobject_psameas_object|190|0|302|0|{Layout|Layout|0|0|}107|49|}
{2010006001|XXGobject_psameas_object|191|0|303|0|{in_metadata|out0_metadata|0|0|}107|37|}
{2010006001|XXGobject_psameas_object|192|0|304|0|{out_metadata|out0_metadata|0|0|}107|37|}
{2010601001|XXGgraph_vertex_vertex|193|0|305|0|{RFMT_V351S3P1_Adaptor_CSV_5010_DNTL_CLMDTL|}1|37|}
{2010601001|XXGgraph_vertex_vertex|194|0|306|0|{RFMT_V353S5P2_V353S6P2Adaptor_DS_CONS_CSV_DENTAL_CLMDTL_HX|}1|113|}
{2010212001|XXGvertex_oport_oport|195|0|308|0|{0|out0|}113|114|}
{2010213001|XXGoport_dst_flow|196|0|310|0|{0|}114|15|}
{2010212001|XXGvertex_oport_oport|197|0|311|0|{1|reject0|}113|115|}
{2010212001|XXGvertex_oport_oport|198|0|313|0|{2|error0|}113|116|}
{2010212001|XXGvertex_oport_oport|199|0|315|0|{3|log|}113|117|}
{2010211001|XXGvertex_iport_iport|200|0|317|0|{0|in|}113|118|}
{2010214001|XXGiport_src_flow|201|0|319|0|{0|}118|16|}
{2010007001|XXGobject_proto_object|202|0|320|0|{}113|119|}
{2010212001|XXGvertex_oport_oport|203|0|322|0|{0|out0|}119|120|}
{2010006001|XXGobject_psameas_object|204|0|324|0|{metadata|out0_metadata|1|0|}120|119|}
{2010006001|XXGobject_psameas_object|205|0|325|0|{transform0|transform0|1|0|}120|119|}
{2010212001|XXGvertex_oport_oport|206|0|326|0|{1|reject0|}119|121|}
{2010006001|XXGobject_psameas_object|207|0|328|0|{metadata|reject0_metadata|1|0|}121|119|}
{2010212001|XXGvertex_oport_oport|208|0|329|0|{2|error0|}119|122|}
{2010006001|XXGobject_psameas_object|209|0|331|0|{metadata|error0_metadata|1|0|}122|119|}
{2010212001|XXGvertex_oport_oport|210|0|332|0|{3|log|}119|123|}
{2010006001|XXGobject_psameas_object|211|0|334|0|{metadata|log_metadata|1|0|}123|119|}
{2010211001|XXGvertex_iport_iport|212|0|335|0|{0|in|}119|124|}
{2010006001|XXGobject_psameas_object|213|0|337|0|{metadata|in_metadata|1|0|}124|119|}
{2010006001|XXGobject_psameas_object|214|0|338|0|{Layout|Layout|0|0|}113|33|}
{2010006001|XXGobject_psameas_object|215|0|339|0|{in_metadata|out0_metadata|0|0|}113|89|}
{2010006001|XXGobject_psameas_object|216|0|340|0|{out0_metadata|write_metadata|0|0|}113|33|}
{2010006001|XXGobject_psameas_object|217|0|341|0|{reject0_metadata|out0_metadata|0|0|}113|89|}
{2010601001|XXGgraph_vertex_vertex|218|0|342|0|{RFMT_V353S6_Xfm_Jnr|}1|89|}
{2010601001|XXGgraph_vertex_vertex|219|0|343|0|{RFMT_V377S0P1_V353S6P3Adaptor_DS_DNTL_CLMDTL_STG|}1|77|}
{2010601001|XXGgraph_vertex_vertex|220|0|344|0|{SORT_V353S0P3_S_Rmv_Dup_keycols|}1|125|}
{2010212001|XXGvertex_oport_oport|221|0|346|0|{0|out|}125|126|}
{2010213001|XXGoport_dst_flow|222|0|348|0|{0|}126|18|}
{2010211001|XXGvertex_iport_iport|223|0|349|0|{0|in|}125|127|}
{2010214001|XXGiport_src_flow|224|0|351|0|{0|}127|14|}
{2010007001|XXGobject_proto_object|225|0|352|0|{}125|128|}
{2010212001|XXGvertex_oport_oport|226|0|354|0|{0|out|}128|129|}
{2010006001|XXGobject_psameas_object|227|0|356|0|{metadata|out_metadata|1|0|}129|128|}
{2010211001|XXGvertex_iport_iport|228|0|357|0|{0|in|}128|130|}
{2010006001|XXGobject_psameas_object|229|0|359|0|{metadata|in_metadata|1|0|}130|128|}
{2010006001|XXGobject_psameas_object|230|0|360|0|{Layout|Layout|0|0|}125|33|}
{2010006001|XXGobject_psameas_object|231|0|361|0|{in_metadata|out0_metadata|0|0|}125|37|}
{2010006001|XXGobject_psameas_object|232|0|362|0|{out_metadata|out0_metadata|0|0|}125|37|}
{2010601001|XXGgraph_vertex_vertex|233|0|363|0|{V351S3P1_CSV_5010_DNTL_CLMDTL|}1|49|}
{2010601001|XXGgraph_vertex_vertex|234|0|364|0|{V353S5_DS_CONS_CSV_DENTAL_CLMDTL_HX|}1|33|}
{2010109001|XXGobject_property_value|235|0|365|0|{0|TrackingThumbprint|72|}1|131|}
