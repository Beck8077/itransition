select t.year, count(*), round(avg(case 
    when price like '$%' then cast(substring(price FROM 2) AS numeric) 
    when price like '€%' then cast(substring(price FROM 2) AS numeric) * 1.2
  end), 2) as average
from task_1 t
group by t.year
order by t.year;