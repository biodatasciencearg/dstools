select t.*,ind.indAU as ModeloAuto 
INTO felitest.dbo.autos_entrenamiento
from (
select a.cuil,bad,'sc3' as scorecard from huevoTest.dbo.score2018_scorecard2_final a
inner join huevoTest.dbo.score2018_poblacionEntrenamientoFinal b
       on a.cuil = b.cuil
UNION
select a.cuil,bad,'sc4' as scorecard from huevoTest.dbo.score2018_scorecard3_nofinanciera a
inner join huevoTest.dbo.score2018_poblacionEntrenamientoFinal b
       on a.cuil = b.cuil
UNION
select a.cuil,bad,'sc5' as scorecard from huevoTest.dbo.score2018_scorecard3_resto a
inner join huevoTest.dbo.score2018_poblacionEntrenamientoFinal b
       on a.cuil = b.cuil
) t 
inner join  MINISIISA_EXTRAS.dbo.indicadoresscore2 ind with (nolock) on t.cuil = ind.cuil and ind.indAU > 0

