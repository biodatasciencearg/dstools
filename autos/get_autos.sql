--autos sc3
select distinct a.cuil,bad,3 as scorecard 
into #tmp_sc3
from huevoTest.dbo.score2018_scorecard2_final a
inner join huevoTest.dbo.score2018_poblacionEntrenamientoFinal b
       on a.cuil = b.cuil 
where a.tieneauto=1
--autos sc4
select distinct a.cuil,bad,4 as scorecard 
into #tmp_sc4
from huevoTest.dbo.score2018_scorecard3_nofinanciera a 
inner join huevoTest.dbo.score2018_poblacionEntrenamientoFinal b
       on a.cuil = b.cuil 
where a.tieneauto=1
--autos sc5
select distinct a.cuil,bad,5 as scorecard 
into #tmp_sc5
from huevoTest.dbo.score2018_scorecard3_resto a
inner join huevoTest.dbo.score2018_poblacionEntrenamientoFinal b
       on a.cuil = b.cuil 
where a.tieneauto=1


SELECT * from #tmp_sc3
union 
SELECT * from #tmp_sc4
union
SELECT * from #tmp_sc5