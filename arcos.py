# Defino algunos strings que puedan llegar a ser utiles.   
sql_diccionario = {
'create_temp_user_table' : {'query':"""create table users_temp (
                                        clientId varchar(25),
                                        mcId varchar(6),
                                        email varchar(50),
                                        userFacebookId varchar(20),
                                        country varchar(2),
                                        firstname varchar(50),
                                        lastname varchar(50),
                                        gender varchar(10),
                                        birthDate timestamp,
                                        documento varchar(20),
                                        createdAt timestamp,
                                        smsEnabled bool,
                                        pushEnabled bool,
                                        emailEnabled bool,
                                        showCouponAlert bool,
                                        currentCity varchar(100),
                                        phoneNumberPrefix varchar(10),
                                        phoneNumberSufix varchar(20),
                                        status varchar(10)
                                        )"""    
                                        ,
                                        'name':'users_temp'
                                        }    
,
'create_temp_coupons_table' : {'query':"""create table coupons_temp(
                                        clientId varchar(255),
                                        campaignId varchar(255),
                                        code varchar(50),
                                        deliveredAt timestamp ,
                                        redeemAt timestamp ,
                                        restaurantKey varchar(50),
                                        restaurantRedeemKey varchar(50)
                                        )"""
                                        ,
                                        'name':'coupons_temp'
                                        }


}


# Defino algunas funciones utiles.  
def consultar_load_tables(tabla):
    import psycopg2
    conn = psycopg2.connect(host="localhost",database="gigigo", user="postgres", password="asdasd")
    # creo el cursor
    cur = conn.cursor()
    # ejecuto y creo tabla temporal
    cur.execute('select count(*) from %s' % sql_diccionario[tabla]['name'])
    rows=cur.fetchall()
    conn.close()
    print("Inserted %i rows in table '%s'" % (rows[0][0],sql_diccionario[tabla]['name']))

    
def drop_tmp_tables(tabla):
    import psycopg2
    conn = psycopg2.connect(host="localhost",database="gigigo", user="postgres", password="asdasd")
    # creo el cursor
    cur = conn.cursor()
    # ejecuto y creo tabla temporal
    cur.execute("drop table if exists %s" % sql_diccionario[tabla]['name'])
    # cargo el archivo en la tabla 
    conn.commit()
    conn.close()
    

def create_load_table_sql(tabla,csv,sep=';'):
    import psycopg2
    conn = psycopg2.connect(host="localhost",database="gigigo", user="postgres", password="asdasd")
    # creo el cursor
    cur = conn.cursor()
    # ejecuto y creo tabla temporal
    cur.execute(sql_diccionario[tabla]['query'])
    # cargo el archivo en la tabla 
    copy_query=("COPY %s FROM STDIN WITH CSV HEADER DELIMITER '%s'" % (sql_diccionario[tabla]['name'],sep))
    with open(csv, 'r+',encoding="utf8") as f:
        cur.copy_expert(copy_query, f)
    conn.commit()
    conn.close()
    
def load_tables(users_table_path,coupons_users_path):
    import arcos as ar
    """Funcion que carga todo a la base de datos tabla de users y de cupones"""
    # Progreso de carga de la base de usuarios.
    print("cargando base de datos de usuarios %s" % users_table_path)
    create_load_table_sql('create_temp_user_table',users_table_path,sep=';')
    consultar_load_tables('create_temp_user_table')
    #
    print("cargando base de datos de cupones %s" % coupons_users_path)
    create_load_table_sql('create_temp_coupons_table',coupons_users_path,sep=',')
    consultar_load_tables('create_temp_coupons_table')
    # Hago algunas cosas aqui.
    #
    # borro las tablas "temporales"
    #drop_tmp_tables('create_temp_user_table')
    #drop_tmp_tables('create_temp_coupons_table')
    
    
def get_column(string):
    '''a partir de un string me devuelve la columna mas parecida'''
    from pyjarowinkler import distance
    columnas   = [(1,"clientId"),(2,"mcId"),(3,"email"),(4,"userFacebookId"),(5,"country"),(6,"firstname"),(7,"lastname"),(8,"gender"),(9,"birthDate"),(10,"documento"),(14,"pushEnabled"),(15,"emailEnabled"),(16,"showCouponAlert"),(20,"versionTyc"),(21,"status")]
    columnas_D = [(columnumber,columname,distance.get_jaro_distance(columname, string, winkler=True, scaling=0.1)) for columnumber,columname in columnas]
    return sorted(columnas_D, key=lambda x: x[2],reverse=True)[0:3]


def remove_non_ascii(text):
    """ Funcion que a partir de un objeto texto te devuelve o vacio o el texto"""
    import re
    import numpy as np
    # pattern de busqueda de mail.
    pat = '^(3[01]|[12][0-9]|0[1-9])/(1[0-2]|0[1-9])/[0-9]{4} (2[0-3]|[01]?[0-9]):([0-5]?[0-9])$'
    # Compile
    p = re.compile(pat)
    if type(text)=='float':
        return None
    else:
        if any([True if ord(e)>128 else False for e in str(text)]):
            return None
        else:
            if bool(p.match(str(text))):
                return text
            else:
                return None
def clean_firstname_lastname(text):
    """ Funcion que a partir de un objeto texto te devuelve o vacio o el texto"""
    import re
    import numpy as np
    if type(text)=='float':
        return None
    else:
        return ''.join(filter(str.isalnum, str(text)))

def clean_manual_report(filename,filenameout):
    """Funcion que permite limpiar campos como createAt,email tengo que pasarle el path completo del file
    ejemplo:
           clean_manual_report('cl_user_0_manual.csv','cl_user_0_manual_clean.csv') """
    import pandas as pd
    #
    # asigno tipos especificos para cada columna
    asignacion_tipos = {"clientId":str,"mcId":str,"email":str,"userFacebookId":str,"country":str,\
                            "firstname":str,"lastname":str,"gender":str,"documento":str,"smsEnabled":str,"pushEnabled":str\
                            ,"emailEnabled":str,"showCouponAlert":str,"currentCity":str,\
                            "phoneNumberPrefix":str,"phoneNumberSufix":str,"status":str,"birthDate":str,"createdAt":str}
    # agrego a la lista para cargar las de tipo fecha.
    columnas_cargadas = list(asignacion_tipos.keys())
    # Cargo el dataframe con las espeficifaciones propiamente dichas.
    df = pd.read_csv(filename,dtype=asignacion_tipos,usecols=columnas_cargadas,error_bad_lines=False,delimiter=',')
    # cambio por nans las fechas de cumpleaños con caracteres raros.
    df.loc[:,'birthDate'] = df.birthDate.apply(lambda x: remove_non_ascii(x))
    # cambio por nans las fechas de creacion con caracteres raros.
    df.loc[:,'createdAt'] = df.createdAt.apply(lambda x: remove_non_ascii(x))
    # Limpio firstname
    df.loc[:,'firstname'] = df.firstname.apply(lambda x: clean_firstname_lastname(x))
    # Limpio lastname
    df.loc[:,'lastname'] = df.lastname.apply(lambda x: clean_firstname_lastname(x))
    # Limpio Documento
    df.loc[:,'documento'] = df.documento.apply(lambda x: clean_firstname_lastname(x))
    # saco los nans
    df.loc[:,'firstname'] = df.firstname.replace('nan',None)
    # 
    df.loc[:,'lastname'] = df.lastname.replace('nan',None)
    #
    df.loc[:,'documento'] = df.documento.replace('nan',None)
    # imprimo el shape del dataframe.
    print("shape antes de limpiar:",df.shape)
    # Busco validar aquellos que tienen mcId correcto.
    correct_mcId = df.mcId.apply(lambda x: True if len(str(x))==6 else False )
    # Hago la interseccion de todos los campos que limpie con la expressiones regulares, y que ademas tengan mcIds sino no los puedo trakear.
    df_clean = df[df.createdAt.notnull() & df.mcId.notnull() & correct_mcId]
    # Me genero el periodo de creacion.
    df_clean.loc[:,'createAtPeriod'] = pd.to_datetime(df_clean.createdAt).dt.strftime('%Y%m')
    df_clean.to_csv(filenameout,index=False,sep=';')
    # imprimo luego de la limpieza
    print("shape despues de limpiar:",df_clean.shape)
    print("Guardado satisfactorialemnte en %s" %(filenameout))
    del df_clean

def get_results_tst_control_rf (sql_table,start_date,end_date):
    """ obtengo los resultados a partir de definir a tablq que tiene que tener el siguiente formato 
    (mcid,rf_score,test_control) Ej:
    5YHB3,11,control
    9OPN2,21,test
    ...etc
    """
    
    query="""select t.rf_score,t.test_control,count(*) tamanio,sum(t.compro) as cnt  from (
        select rf.*,CASE  WHEN c.redeemat is null  THEN 0  WHEN c.redeemat is not null THEN 1 END as compro
        from %s  rf 
        --join con el clientid
        left join users_clientid u 
                on u.mcid=rf.mcid
        --join con los grupos que mande.
        left join (select * from coupons_temp c where c.redeemat between '%s'::timestamp and  '%s'::timestamp and c.redeemat is not null) c
                on c.clientid=u.clientid
        ) t 
    group by t.rf_score,t.test_control
    """ % (sql_table,start_date,end_date)
    import psycopg2
    import pandas as pd
    conn = psycopg2.connect(host="localhost",database="gigigo", user="postgres", password="asdasd")
    # creo el cursor
    cur = conn.cursor()
    # ejecuto y creo tabla temporal
    cur.execute(query)
    results = cur.fetchall()
    df = pd.DataFrame(results,columns=['rf_score', 'test_control', 'tamanio', 'cnt'])
    # reshape de la data.
    df=pd.DataFrame(df.pivot(index='rf_score',columns='test_control',values=['cnt','tamanio']).reset_index().values,columns=['rf_score','control','test','cnt_size','test_size'])
    # Agrego totales
    df = df.append(pd.DataFrame({'rf_score':'total','control':df.control.sum(),'test':df.test.sum(),'cnt_size':df.cnt_size.sum(),'test_size':df.test_size.sum()},index=[0]))
    # calculo el cociente.
    import numpy as np
    df['ratio']=df.apply(lambda x: np.nan if x['control']==0 or x['test']==0 else (x['test']/x['test_size'])/(x['control']/x['cnt_size']),axis=1)    
    # calculo la elevacion.
    df['lift'] = (df.ratio-1)*100
    df.columns=['rf_score','GCs grp_control','GCs grp_test','grp_control_size','grp_test_size','ratio','lift %']
    return df



# obtengo un string con la fecha de ultima modificacion.
def get_date_ffile(file_name):
    """Funcion que devuelve la fecha en formato yyyymmdd de un archivo de su ultima modificacion"""
    from datetime import datetime
    import os
    try:
        mtime = os.path.getmtime(file_name)
    except OSError:
        mtime = 0
    last_modified_date = datetime.fromtimestamp(mtime).strftime("%Y%m%d")
    return last_modified_date
# Fucion que devuelve los archivos y su fecha de modificacion de gigigo.
def get_files_list(atribute='User',country='AR'):
    """extraigo todos lo archivos con un dado atributo
    opciones=['User','Campaign',  'Campaign vs Tag',   'Coupon',   'User', 'User vs Tag'']"""
    # Defino el path de gigigo.
    gigigo_path = "C:/Users/elopez/Golden Arch Development Corp/Transferencia Bases de Datos - Gigigo"
    # importo glob.
    import glob
    # me busco todos los zip de las subfolders.
    absolute_path = gigigo_path +  '/' + country+ '/' + atribute + '/*.zip'
    # guardo en una lista todos los files.
    absolute_path_filenames = glob.glob(absolute_path)
    # Me guardo los nombre de archivos.
    filenames = ([file.split('\\')[-1] for file in absolute_path_filenames])
    # itero sobre las fechas de modificacion.
    name_date = [(file,get_date_ffile(modification_date),modification_date) for modification_date,file in zip(absolute_path_filenames,filenames)]
    #
    return (name_date)

def run_sp(query):
    import psycopg2
    conn = psycopg2.connect(host="localhost",database="gigigo", user="postgres", password="asdasd")
    # creo el cursor
    cur = conn.cursor()
    # ejecuto y creo tabla temporal
    cur.execute(query)
    #rows=cur.fetchall()
    conn.commit()
    conn.close()

def run_query(query):
    import psycopg2
    conn = psycopg2.connect(host="localhost",database="gigigo", user="postgres", password="asdasd")
    # creo el cursor
    cur = conn.cursor()
    # ejecuto y creo tabla temporal
    cur.execute(query)
    rows=cur.fetchall()
    conn.close()
    return rows

    
    
    
    
def clean_file(filename,atribute='User'):
    """Funcion que limpia el archivo de entrada tribute:['User','Coupon']"""
    import pandas as pd
    from zipfile import ZipFile
    import csv
    import os
    filename = r"{}".format(filename)
    # consigo el nombre base del archivo.
    base = os.path.splitext(os.path.basename(filename))[0]
    # abro el zip.
    
    zip_file = ZipFile(r"{}".format(filename))
    # separador:
    separador = get_delimiter(filename)
    if atribute=='User':
        columnas=['clientId', 'mcId', 'email', 'userFacebookId', 'country', 'firstname',
                  'lastname', 'gender', 'birthDate', 'documento',   'createdAt', 'smsEnabled',
                  'pushEnabled', 'emailEnabled', 'showCouponAlert', 'currentCity', 'phoneNumberPrefix','phoneNumberSufix', 
                  'status']
        dtypes={'clientId':str, 'mcId':str, 'email':str, 'userFacebookId':str, 'country':str, 'firstname':str,
                'lastname':str, 'gender':str, 'documento':str,  'smsEnabled':str,
                'pushEnabled':str, 'emailEnabled':str, 'showCouponAlert':str, 'currentCity':str, 'phoneNumberPrefix':str,
                'phoneNumberSufix':str, 
                  'status':str}
        parse_dates = ['birthDate','createdAt']
        # cargo el dataset en memoria.
        df = pd.read_csv(zip_file.open(base + '.csv'),sep=separador,dtype=dtypes,usecols=columnas,parse_dates=parse_dates,error_bad_lines=False)
        # limpio el df
        df_clean = df [( df['clientId'].str.len() <= 25.0 ) &
        ( df['mcId'].str.len() <= 6.0 )            &
        ( df['email'].str.len() <= 50.0 )          &
        (( df['userFacebookId'].str.len() <= 20.0 ) | ( df['userFacebookId'].isnull() )  ) &
        ( df['country'].str.len() <= 2.0 )         &
        (( df['firstname'].str.len() <= 50.0 ) | ( df['firstname'].isnull() )  ) &
        (( df['lastname'].str.len() <= 50.0 ) | ( df['lastname'].isnull() )  ) &
        (( df['gender'].str.len() <= 10.0 ) | ( df['gender'].isnull() )  ) &
        (( df['documento'].str.len() <= 20.0 ) | ( df['documento'].isnull() )  ) &
        ( df['smsEnabled'].str.len() <= 5.0 )      &
        ( df['pushEnabled'].str.len() <= 5.0 )     &
        ( df['emailEnabled'].str.len() <= 5.0 )    &
        ( df['showCouponAlert'].str.len() <= 5.0 ) &
        (( df['currentCity'].str.len() <= 100.0 ) | ( df['currentCity'].isnull() )  ) &
        (( df['phoneNumberPrefix'].str.len() <= 10.0 ) | ( df['phoneNumberPrefix'].isnull() )  ) &
        (( df['phoneNumberSufix'].str.len() <= 20.0 ) | ( df['phoneNumberSufix'].isnull() )  ) &
        (( df['status'].str.len() <= 10.0 ) | ( df['status'].isnull() )  ) ][['clientId', 'mcId', 'email', 'userFacebookId', 'country','firstname','lastname', 'gender', 'documento', 'createdAt','smsEnabled', 'pushEnabled', 'emailEnabled', 'showCouponAlert','currentCity', 'phoneNumberPrefix', 'phoneNumberSufix', 'status']]
        df_clean['birthDate']=pd.to_datetime(df.birthDate,errors='coerce')
        return (len(df),len(df_clean),df_clean[['clientId', 'mcId', 'email', 'userFacebookId', 'country','firstname','lastname', 'gender', 'birthDate', 'documento', 'createdAt','smsEnabled', 'pushEnabled', 'emailEnabled', 'showCouponAlert','currentCity', 'phoneNumberPrefix', 'phoneNumberSufix', 'status']])
    if atribute=='Coupon':
        # defino tipo de archivos.
        dtypes={'clientId':str,'campaignId':str, 'code':str, 'restaurantKey':str,'restaurantRedeemKey':str}
        # cargo el df 
        df=pd.read_csv(zip_file.open(base + '.csv'),dtype=dtypes,parse_dates=['deliveredAt','redeemAt'],sep=separador,error_bad_lines=False)
        # me quedo con cupones redimidos y con clientid.
        df_clean = df[(df.clientId.notnull()) & (df.redeemAt.notnull())]
        # retorno lo que me interesa
        return (len(df),len(df_clean),df_clean)
    else:
        print('Debe colocar un atributo válido')

        
        
def updatedb(atributo='User',country='AR'):
    """Funcion para actualizar la base de datos"""
    import os
    # listado de archivos que busco actualizar.
    archivos = get_files_list(atributo,country)
    #print(archivos[1:3])
    lst=archivos
    for (shortname,date,filename) in lst:
        # verifico la existencia de ese archivo en la base.
        exists_file = f"select case when count(*) <> 0 then 'EXISTE' else 'INEXISTENTE' end from updated_tables ut where ut.filename='{shortname}' and ut.fecha='{date}'"
        # verifico el estatus de mi archivo 
        status = run_query(exists_file)[0][0]
        if status=='EXISTE':
            print(f'skipeando {shortname} porque ya esta actualizado.')
        elif status=='INEXISTENTE':
            #
            try:
                print(f'Limpiando y formateando {shortname}')
                # intento cargar el archivo.
                antes,despues,df=clean_file(filename,atributo)
                # Guardo el archivo en disco
                print(f"\n\t*Guardando archivo {shortname}, size inicial:{antes}, size_final:{despues} ")
                df.to_csv('tmp.csv',index=False,sep='|')
                if atributo=='User':
                    sp_create_tmp_table = 'create_temp_user_table'
                    tabla=sql_diccionario[sp_create_tmp_table]['name']
                elif atributo=='Coupon':
                    sp_create_tmp_table = 'create_temp_coupons_table'
                    tabla=sql_diccionario[sp_create_tmp_table]['name']
                #
                print(f'\n\t\t*Cargando {shortname} a {tabla}')
                create_load_table_sql(sp_create_tmp_table,'tmp.csv',sep='|')
                #
                print(f'\n\t\t\t*Actualizando ... {atributo} en DB.')
                if atributo=='User':
                    run_sp(f'call sp_get_exclusion_list();')
                    run_sp(f'call sp_update_user_table();')
                elif atributo=='Coupon':
                    run_sp(f"call sp_update_cupones('{country}');")
                #
                #
                print(f'\n\t\t\t\t*Borrando {tabla} de la DB.\n Done!')
                run_sp(f'drop table if exists  "{tabla}";')

                # actualizo la tabla donde me dice que archivos se actualizaron.
                insert_string = f"insert into updated_tables(filename,fecha)(select '{shortname}','{date}' where not exists (select 1 from updated_tables ut where ut.filename = '{shortname}' and ut.fecha =  '{date}'));"
                # inserto la nueva row.
                run_sp(insert_string)
                # borro el archivo temporal 
                os.remove('tmp.csv')
            except Exception:
                print(f"An exception occurred in file {shortname} skipping...")
                pass
                                         
        #
        
def get_delimiter(filename):
    "Retorno si el csv esta con comma o punto y comma"
    import zipfile
    import csv
    import os
    # consigo el nombre base del archivo.
    base = os.path.splitext(os.path.basename(filename))[0]
    # abro el csv y consigo la primer linea.
    with zipfile.ZipFile(filename) as z:
        with z.open(base + '.csv') as f:
            f_line= f.readline().decode("utf-8") 
    # inicializo el sniffer
    sniffer = csv.Sniffer()
    dialect = sniffer.sniff(f_line)
    # retorno el separador.
    return dialect.delimiter