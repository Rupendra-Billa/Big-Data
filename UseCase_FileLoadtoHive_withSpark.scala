package com.Bigdata.spark.scd

 

import com.typesafe.config.ConfigFactory

import org.apache.spark._

import org.apache.spark.sql.SaveMode

 

object SCDDiffUtility {

 

  def main(args: Array[String]): Unit = {

 

    val startTime = System.currentTimeMillis()

 

    val sparkConf = new SparkConf().setAppName("SCDUtility")

      .set("spark.shuffle.service.enabled", "false")

      .set("spark.dynamicAllocation.enabled", "false")

      .set("spark.io.compression.codec", "snappy")

      .set("spark.rdd.compress", "true")

 

    val sc = new SparkContext(sparkConf)

 

    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    import hiveContext.implicits._

    hiveContext.setConf("hive.exec.dynamic.partition", "true")

    hiveContext.setConf("hive.exec.dynamic.partition.mode", "true")

    hiveContext.setConf("hive.execution.engine", "spark")

    hiveContext.setConf("hive.vectorized.execution.enabled", "true")

    hiveContext.setConf("hive.vectorized.execution.reduce.enabled", "true")

 

    // Constants

    val today = new java.text.SimpleDateFormat("yyyy-MM-dd").format(new java.util.Date())

    val spark_end_date_constant = "9999-12-31"

 

    // History columns

    val hist_columns = List("spark_audit_id", "spark_effective_date", "spark_end_date", "spark_delete_date", "spark_cmcif_party_sid", "cif_no", "legal_name", "alias_name", "f_name", "m_name", "l_name", "dob", "nationality", "chng_count", "mkr_id", "mkr_dt", "auth_id", "auth_dt", "status", "br_cd", "pos_cd", "ref_no", "legacy_cif", "pefp_designated", "pep_designated", "client_since", "entity_status", "family_name", "customer_classification", "precheck_date", "amlcheckdone", "business", "occupation", "ownership_type", "previous_name", "name_change_dt", "cust_type", "title", "suffix", "offical_lang", "pref_lang", "occupation_desc", "doc_id", "open_date", "privacy_flg", "citizenship", "created_lob", "pep_dt", "pep_memo", "local_title", "local_legal_name", "local_f_name", "local_m_name", "local_l_name", "local_family_name", "owning_by_lob", "org_cert_code", "previous_local_name", "sanction", "sanction_desc", "sanction_dt", "pep_justification", "us_tax_id", "entity_status_dt_chng", "kyc_status")

 

    // Delta columns

    val delta_columns = List("spark_warehouse_id", "delta_spark_audit_id", "delta_spark_effective_date", "delta_spark_end_date", "delta_spark_delete_date", "delta_spark_cmcif_party_sid", "delta_cif_no", "delta_legal_name", "delta_alias_name", "delta_f_name", "delta_m_name", "delta_l_name", "delta_dob", "delta_nationality", "delta_chng_count", "delta_mkr_id", "delta_mkr_dt", "delta_auth_id", "delta_auth_dt", "delta_status", "delta_br_cd", "delta_pos_cd", "delta_ref_no", "delta_legacy_cif", "delta_pefp_designated", "delta_pep_designated", "delta_client_since", "delta_entity_status", "delta_family_name", "delta_customer_classification", "delta_precheck_date", "delta_amlcheckdone", "delta_business", "delta_occupation", "delta_ownership_type", "delta_previous_name", "delta_name_change_dt", "delta_cust_type", "delta_title", "delta_suffix", "delta_offical_lang", "delta_pref_lang", "delta_occupation_desc", "delta_doc_id", "delta_open_date", "delta_privacy_flg", "delta_citizenship", "delta_created_lob", "delta_pep_dt", "delta_pep_memo", "delta_local_title", "delta_local_legal_name", "delta_local_f_name", "delta_local_m_name", "delta_local_l_name", "delta_local_family_name", "delta_owning_by_lob", "delta_org_cert_code", "delta_previous_local_name", "delta_sanction", "delta_sanction_desc", "delta_sanction_dt", "delta_pep_justification", "delta_us_tax_id", "delta_entity_status_dt_chng", "delta_kyc_status")

 

    val target_table_name = ConfigFactory.load().getString("scd.target_table.value")

    val source_table_name = ConfigFactory.load().getString("scd.source_table.value")

 

    val source_df = hiveContext.table(source_table_name)

    val target_df = hiveContext.table(target_table_name)

    val target_current_active_df = target_df.where($"spark_end_date" === spark_end_date_constant).cache()

    val target_non_current_active_df = target_df.where($"spark_end_date" !== spark_end_date_constant)

 

 

    import org.apache.spark.sql.functions._

    // To find all the new and changed records

 

    val delta_source_df = source_df.withColumnRenamed("spark_audit_id", "delta_spark_audit_id").withColumnRenamed("spark_effective_date", "delta_spark_effective_date").withColumnRenamed("spark_end_date", "delta_spark_end_date").withColumnRenamed("spark_delete_date", "delta_spark_delete_date").withColumnRenamed("spark_cmcif_party_sid", "delta_spark_cmcif_party_sid").withColumnRenamed("cif_no", "delta_cif_no").withColumnRenamed("legal_name", "delta_legal_name").withColumnRenamed("alias_name", "delta_alias_name").withColumnRenamed("f_name", "delta_f_name").withColumnRenamed("m_name", "delta_m_name").withColumnRenamed("l_name", "delta_l_name").withColumnRenamed("dob", "delta_dob").withColumnRenamed("nationality", "delta_nationality").withColumnRenamed("chng_count", "delta_chng_count").withColumnRenamed("mkr_id", "delta_mkr_id").withColumnRenamed("mkr_dt", "delta_mkr_dt").withColumnRenamed("auth_id", "delta_auth_id").withColumnRenamed("auth_dt", "delta_auth_dt").withColumnRenamed("status", "delta_status").withColumnRenamed("br_cd", "delta_br_cd").withColumnRenamed("pos_cd", "delta_pos_cd").withColumnRenamed("ref_no", "delta_ref_no").withColumnRenamed("legacy_cif", "delta_legacy_cif").withColumnRenamed("pefp_designated", "delta_pefp_designated").withColumnRenamed("pep_designated", "delta_pep_designated").withColumnRenamed("client_since", "delta_client_since").withColumnRenamed("entity_status", "delta_entity_status").withColumnRenamed("family_name", "delta_family_name").withColumnRenamed("customer_classification", "delta_customer_classification").withColumnRenamed("precheck_date", "delta_precheck_date").withColumnRenamed("amlcheckdone", "delta_amlcheckdone").withColumnRenamed("business", "delta_business").withColumnRenamed("occupation", "delta_occupation").withColumnRenamed("ownership_type", "delta_ownership_type").withColumnRenamed("previous_name", "delta_previous_name").withColumnRenamed("name_change_dt", "delta_name_change_dt").withColumnRenamed("cust_type", "delta_cust_type").withColumnRenamed("title", "delta_title").withColumnRenamed("suffix", "delta_suffix").withColumnRenamed("offical_lang", "delta_offical_lang").withColumnRenamed("pref_lang", "delta_pref_lang").withColumnRenamed("occupation_desc", "delta_occupation_desc").withColumnRenamed("doc_id", "delta_doc_id").withColumnRenamed("open_date", "delta_open_date").withColumnRenamed("privacy_flg", "delta_privacy_flg").withColumnRenamed("citizenship", "delta_citizenship").withColumnRenamed("created_lob", "delta_created_lob").withColumnRenamed("pep_dt", "delta_pep_dt").withColumnRenamed("pep_memo", "delta_pep_memo").withColumnRenamed("local_title", "delta_local_title").withColumnRenamed("local_legal_name", "delta_local_legal_name").withColumnRenamed("local_f_name", "delta_local_f_name").withColumnRenamed("local_m_name", "delta_local_m_name").withColumnRenamed("local_l_name", "delta_local_l_name").withColumnRenamed("local_family_name", "delta_local_family_name").withColumnRenamed("owning_by_lob", "delta_owning_by_lob").withColumnRenamed("org_cert_code", "delta_org_cert_code").withColumnRenamed("previous_local_name", "delta_previous_local_name").withColumnRenamed("sanction", "delta_sanction").withColumnRenamed("sanction_desc", "delta_sanction_desc").withColumnRenamed("sanction_dt", "delta_sanction_dt").withColumnRenamed("pep_justification", "delta_pep_justification").withColumnRenamed("us_tax_id", "delta_us_tax_id").withColumnRenamed("entity_status_dt_chng", "delta_entity_status_dt_chng").withColumnRenamed("kyc_status", "delta_kyc_status")

 

    // To find new record

    val cdc_new_record_df = delta_source_df.join(target_current_active_df, $"cif_no" === $"delta_cif_no", "left_outer").where(isnull($"spark_audit_id"))

 

    // To find changed record

    val cdc_changed_df = delta_source_df.join(target_current_active_df, $"cif_no" === $"delta_cif_no", "left_outer").where(($"delta_legal_name" !== $"legal_name") or ($"delta_alias_name" !== $"alias_name") or ($"delta_f_name" !== $"f_name") or ($"delta_m_name" !== $"m_name") or ($"delta_l_name" !== $"l_name") or ($"delta_dob" !== $"dob") or ($"delta_nationality" !== $"nationality") or ($"delta_chng_count" !== $"chng_count") or ($"delta_mkr_id" !== $"mkr_id") or ($"delta_mkr_dt" !== $"mkr_dt") or ($"delta_auth_id" !== $"auth_id") or ($"delta_auth_dt" !== $"auth_dt") or ($"delta_status" !== $"status") or ($"delta_br_cd" !== $"br_cd") or ($"delta_pos_cd" !== $"pos_cd") or ($"delta_ref_no" !== $"ref_no") or ($"delta_legacy_cif" !== $"legacy_cif") or ($"delta_pefp_designated" !== $"pefp_designated") or ($"delta_pep_designated" !== $"pep_designated") or ($"delta_client_since" !== $"client_since") or ($"delta_entity_status" !== $"entity_status") or ($"delta_family_name" !== $"family_name") or ($"delta_customer_classification" !== $"customer_classification") or ($"delta_precheck_date" !== $"precheck_date") or ($"delta_amlcheckdone" !== $"amlcheckdone") or ($"delta_business" !== $"business") or ($"delta_occupation" !== $"occupation") or ($"delta_ownership_type" !== $"ownership_type") or ($"delta_previous_name" !== $"previous_name") or ($"delta_name_change_dt" !== $"name_change_dt") or ($"delta_cust_type" !== $"cust_type") or ($"delta_title" !== $"title") or ($"delta_suffix" !== $"suffix") or ($"delta_offical_lang" !== $"offical_lang") or ($"delta_pref_lang" !== $"pref_lang") or ($"delta_occupation_desc" !== $"occupation_desc") or ($"delta_doc_id" !== $"doc_id") or ($"delta_open_date" !== $"open_date") or ($"delta_privacy_flg" !== $"privacy_flg") or ($"delta_citizenship" !== $"citizenship") or ($"delta_created_lob" !== $"created_lob") or ($"delta_pep_dt" !== $"pep_dt") or ($"delta_pep_memo" !== $"pep_memo") or ($"delta_local_title" !== $"local_title") or ($"delta_local_legal_name" !== $"local_legal_name") or ($"delta_local_f_name" !== $"local_f_name") or ($"delta_local_m_name" !== $"local_m_name") or ($"delta_local_l_name" !== $"local_l_name") or ($"delta_local_family_name" !== $"local_family_name") or ($"delta_owning_by_lob" !== $"owning_by_lob") or ($"delta_org_cert_code" !== $"org_cert_code") or ($"delta_previous_local_name" !== $"previous_local_name") or ($"delta_sanction" !== $"sanction") or ($"delta_sanction_desc" !== $"sanction_desc") or ($"delta_sanction_dt" !== $"sanction_dt") or ($"delta_pep_justification" !== $"pep_justification") or ($"delta_us_tax_id" !== $"us_tax_id") or ($"delta_entity_status_dt_chng" !== $"entity_status_dt_chng"))

 

    //val cdc_all_record_df = cdc_new_record_df.unionAll(cdc_changed_df)

 

    // The newly changed records with delta columns

    val cdc_changed_to_insert_df = cdc_changed_df.select(delta_columns.head, delta_columns.tail: _*)

 

    // New record from source table

    val cdc_new_to_insert_df = cdc_new_record_df.select(delta_columns.head, delta_columns.tail: _*).withColumn("spark_warehouse_id", lit("25664240"))

 

    val cif_no_list = cdc_changed_df.select("cif_no").rdd.map(r => r(0)).collect

 

    val hist_with_expired_records_df = target_current_active_df.withColumn("spark_end_date", lit(today)).withColumn("spark_delete_date", lit(today)).where($"cif_no".isin(cif_no_list: _*)).unionAll(target_current_active_df.where(!$"cif_no".isin(cif_no_list: _*)))

 

    val ready_to_db = hist_with_expired_records_df.unionAll(cdc_changed_to_insert_df).unionAll(cdc_new_to_insert_df).unionAll(target_non_current_active_df)

 

    ready_to_db.write.mode(SaveMode.Overwrite).saveAsTable("hdp_spark_dev_l15__hdp_cmcif.l1_cmcif_gr110mb_temp_target")

 

   val totalTime = System.currentTimeMillis() - startTime

 

    println("############## Time Elapsed: %1d ms".format(totalTime))

 

    sc.stop()

  }

}