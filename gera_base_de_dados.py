def get_time():
    from datetime import datetime
    from pytz import timezone
    return datetime.now().astimezone(timezone("America/Sao_Paulo")).strftime("%Y_%m_%d_%H_%M_%S")

def run_transform_to_qgis():
    spark.sql(f"""
    SELECT ano, sig_secao_ori_des, origem, destino, SUM(passageiros_total) AS passageiros FROM
    (SELECT
    CAST(ano AS STRING) AS ano,
    CAST(modalidade AS STRING) AS modalidade,
    CAST(sig_secao_ori AS STRING) AS origem,
    CAST(sig_secao_des AS STRING) AS destino,
    CONCAT(sig_secao_ori, ' - ', sig_secao_des) AS sig_secao_ori_des,
    CAST(passageiros_total AS INTEGER) AS passageiros_total
    FROM data
    WHERE passageiros_total > 0 AND NOT sig_secao_ori = sig_secao_des AND NOT (LENGTH(sig_secao_ori) = 2 OR LENGTH(sig_secao_des) = 2)
    UNION ALL
    SELECT
    CAST(ano AS STRING) AS ano,
    CAST(modalidade AS STRING) AS modalidade,
    CAST(sig_secao_des AS STRING) AS origem,
    CAST(sig_secao_ori AS STRING) AS destino,
    CONCAT(sig_secao_des, ' - ', sig_secao_ori) AS sig_secao_ori_des,
    CAST(passageiros_total AS INTEGER) AS passageiros_total
    FROM data
    WHERE passageiros_total > 0 AND NOT sig_secao_ori = sig_secao_des AND NOT (LENGTH(sig_secao_ori) = 2 OR LENGTH(sig_secao_des) = 2))
    GROUP BY ano, sig_secao_ori_des, origem, destino
    ORDER BY ano ASC, sig_secao_ori_des ASC
    """).drop("sig_secao_ori_des").repartition(1).write.option("header", "true").option("sep", ";").option("encoding", "utf-8").csv(f"./{get_time()}_santa_catarina_fluxos_ori_des_qgis/")

if __name__ == "__main__":
    import findspark
    findspark.init()
    from pyspark.sql import SparkSession
    from path import path_ref_join_parquet
    
    spark = SparkSession.builder.master('local[2]').appName("tratamento_datasets_sie").getOrCreate()
    df_spark = spark.read.parquet(path_ref_join_parquet)
    df_spark.createOrReplaceTempView("data")
    run_transform_to_qgis()
    input()