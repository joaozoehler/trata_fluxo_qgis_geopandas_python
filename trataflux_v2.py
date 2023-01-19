def get_shape_flow(from_arq_latlon, from_arq_fluxos, to_arq_shp, to_arq_geojson, to_arq_csv):
    import os
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import LineString, Point
    from shapely import wkt

    print("Verificando (e criando) a pasta /shp")
    if not os.path.exists("./shp"):
        os.mkdir("./shp")
    print("Verificando (e criando) a pasta /geojson")
    if not os.path.exists("./geojson"):
        os.mkdir("./geojson")
    print("Verificando (e criando) a pasta /csv")
    if not os.path.exists("./csv"):
        os.mkdir("./csv")

    print("Lendo o dataset com latitude e longitude")
    df_latlon_ori = pd.read_csv(from_arq_latlon, index_col = False)
    df_latlon_ori["LATITUDE"] = df_latlon_ori["LATITUDE"].astype("str")
    df_latlon_ori["LONGITUDE"] = df_latlon_ori["LONGITUDE"].astype("str")
    df_latlon_ori["origem_latlon"] = "POINT (" + df_latlon_ori["LONGITUDE"] + " " + df_latlon_ori["LATITUDE"] + ")"
    df_latlon_ori.drop(["NOMESIG", "NOME", "UF", "GEOCODIGO", "LATITUDEGM", "LONGITUDEG", "RGINT", "RGI", "LATITUDE", "LONGITUDE"], axis = 1, inplace = True)
    df_latlon_ori.rename({"NOMESIGMAI" : "origem"}, axis = 1, inplace = True)

    df_latlon_des = pd.read_csv(from_arq_latlon, index_col = False)
    df_latlon_des["LATITUDE"] = df_latlon_des["LATITUDE"].astype("str")
    df_latlon_des["LONGITUDE"] = df_latlon_des["LONGITUDE"].astype("str")
    df_latlon_des["destino_latlon"] = "POINT (" + df_latlon_des["LONGITUDE"] + " " + df_latlon_des["LATITUDE"] + ")"
    df_latlon_des.drop(["NOMESIG", "NOME", "UF", "GEOCODIGO", "LATITUDEGM", "LONGITUDEG", "RGINT", "RGI", "LATITUDE", "LONGITUDE"], axis = 1, inplace = True)
    df_latlon_des.rename({"NOMESIGMAI" : "destino"}, axis = 1, inplace = True)

    print("Lendo o dataset com os dados de movimentação de passageiros")
    df_left = pd.read_csv(from_arq_fluxos, index_col = False, sep = ";")

    print("Realizando join entre dataset de movimentação + dataset latlon")
    df_left = df_left.merge(df_latlon_ori, on = "origem", how = "left")
    df_left = df_left.merge(df_latlon_des, on = "destino", how = "left")

    df_left["origem_latlon"] = df_left["origem_latlon"].apply(wkt.loads)
    df_left["destino_latlon"] = df_left["destino_latlon"].apply(wkt.loads)

    print("Criando a coluna linestring no dataset de movimentação de passageiros")
    df_left["linestring"] = df_left.apply(lambda row: LineString([row["origem_latlon"], row["destino_latlon"]]), axis = 1)

    print("Criando GeoDataFrame com GeoPandas")
    df_geo = gpd.GeoDataFrame(df_left[["ano", "origem", "destino", "passageiros"]], geometry = df_left["linestring"], crs='epsg:4674')

    df_geo["passageiros"] = gpd.pd.to_numeric(df_geo["passageiros"], downcast = "integer")

    print("Criando coluna id para remoção das linhas duplicadas")
    df_geo["id"] = (df_geo["ano"]).astype("str") + (df_geo["passageiros"] * (df_geo.length * 10)).astype("str").str.replace(".", "", regex = True)

    df_geo["origem_destino"] = df_geo["origem"] + " - " + df_geo["destino"]

    print("Removendo as linhas duplicadas")
    df_geo.drop_duplicates(["id"], keep = "first", inplace = True)

    df_geo.drop(["id"], axis = 1, inplace = True)

    print("Salvando o arquivo no formato shapefile")
    df_geo.to_file("./shp/" + to_arq_shp, encoding = "utf-8")
    
    print("Salvando o arquivo no formato GeoJSON")
    df_geo.to_file("./geojson/" + to_arq_geojson, driver = "GeoJSON", encoding = "utf-8")
    
    print("Salvando o arquivo no formato CSV")
    df_geo.to_csv("./csv/" + to_arq_csv, encoding = "utf-8")
    
    print("Tarefa concluída.")
    input()
    
if __name__ == "__main__":
    from path import latlon, fluxos, shp, geojson, csv
    
    get_shape_flow(latlon, fluxos, shp, geojson, csv)