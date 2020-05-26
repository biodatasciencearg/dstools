--busco la table de vehiculos para la que es titular un dado cuil 
--20188740449 jairo
--20300827854
--20263649894
declare @cuil NUMERIC(11) = 20263649894;
SELECT t.cuil,t.DOMINIO,t.MARCA,t.MODELOVERS,t.TIPOVEHIC,t.ANO into #auto_table from (
    SELECT a.cuil,a.DOMINIO,a.TIPOVEHIC,a.MARCA,a.MODELOVERS,a.ANO
    ,ROW_NUMBER() over (partition by a.DOMINIO
                            order by a.DOMINIO) RowNumber 
    from MINISIISA_EXTRAS.dbo.autosal022020 a with (nolock)
    where a.cuil=@cuil and not exists (select 1 from MINISIISA_EXTRAS.dbo.autosal022020 b 
        with (nolock) where b.DOMINIO=a.DOMINIO and b.fecha > a.fecha
        ) and a.TIPOVEHIC is not NULL and a.DOMINIO is not null
) t 
where t.RowNumber =1 
order by t.ANO


select * from #auto_table
drop table if exists #auto_table



