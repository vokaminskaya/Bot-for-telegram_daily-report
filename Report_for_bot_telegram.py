# Задача: Подготовить полноценный отчет по приложению. 
# Данный отчет должен отправляться в чат каждый день в 11:30.
# Отчет должен включать в себя следующие данные:
# Текстом: 
# - DAU
# - likes & wiews
# - CTR
# - sms/users (кол-во сообщений на юзера, которые их отправляют)
# - Кол-во сообщений
# - Кол-во юзеров, отправивших сообщения
# Графиками:
# - likes & views & messages
# - CTR
# - Retention
# - Кол-во новых юзеров/оставшихся/ушедших


# Импортируем все необходимое
import telegram
import numpy as np
import pandas as pd
import pandahouse as ph

import matplotlib.pyplot as plt
import seaborn as sns
import io
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Для подключения к бд
connection = {'host': *,
              'password': *,
              'user': *,
              'database': *}


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': *,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2022, 11, 14)}

my_token = *
chat_id = *
        
bot = telegram.Bot(token=my_token)


@dag(default_args=default_args, schedule_interval='30 11 * * *', catchup=False)
def full_report_telegram_kaminskaya():
    
        
    @task
    # Загружаем данные за прошлый день
    def extract_yesterday():
        query_1 = """SELECT max(toDate(time)) as day, 
                count(DISTINCT user_id) as DAU, 
                sum(action = 'like') as likes,
                sum(action = 'view') as views, 
                round(likes/views*100, 2) as CTR
                FROM {db}.feed_actions
                WHERE toDate(time) = yesterday()"""

        report_yesterday = ph.read_clickhouse(query_1, connection=connection)
        return report_yesterday
    
    
    @task
    # Загружаем данные за прошлый день (сообщения)
    def extract_yesterday_sms():
        query_2 = """SELECT max(toDate(time)) as day,
        count(user_id) as count_sms,  
        count(DISTINCT user_id) as count_user_sms, 
        round(count_sms/count_user_sms, 2) as sms_per_user
        FROM {db}.message_actions
        WHERE toDate(time) = yesterday()"""

        report_yesterday_sms = ph.read_clickhouse(query_2, connection=connection)
        return report_yesterday_sms
    
    
    @task
    # Готовим сообщение для телеграма
    def prepare_message(report_yesterday, report_yesterday_sms):
        message = f'''
        Ключевые метрики за {str(report_yesterday.iat[0, 0])[:-9]}
---
DAU: {str(report_yesterday.iat[0, 1])}

likes: {str(report_yesterday.iat[0, 2])}

views: {str(report_yesterday.iat[0, 3])}

CTR: {str(report_yesterday.iat[0, 4])}%
---
Количество отправленных сообщений: {str(report_yesterday_sms.iat[0, 1])}

Количество пользователей, отправивших сообщения: {str(report_yesterday_sms.iat[0, 2])}

Среднее кол-во сообщений на пользователя: {str(report_yesterday_sms.iat[0, 3])}'''
        return message
    
    
    @task
    # Загружаем данные о CTR за последние 10 дней
    def extract_ctr_10_days():
        query_3 = """SELECT toDate(time) as day, 
                round(sum(action = 'like')/sum(action = 'view')*100, 2) as CTR
                FROM {db}.feed_actions
                WHERE toDate(time) > today()-11 AND toDate(time) < today()
                GROUP BY day"""

        report_10_days_ctr = ph.read_clickhouse(query_3, connection=connection)
        return report_10_days_ctr    
        

    @task
    # Загружаем данные за последние 10 дней (кол-во сообщений на юзера)
    def extract_sms_per_user_10_days():
        query_4 = """SELECT toDate(time) as day,
                round(count(user_id)/count(DISTINCT user_id), 2) as sms_per_user
                FROM {db}.message_actions
                WHERE toDate(time) > today()-11 AND toDate(time) < today()
                GROUP BY day"""

        report_10_days_sms = ph.read_clickhouse(query_4, connection=connection)
        return report_10_days_sms
        
        
    @task
    # данные по лайкам, просмотрам и смс за последние 10 дней
    def extract_feed_10_days():        
        query_5 = """select day, action, count(DISTINCT user_id) as count_user
                    from
                    (select user_id, 'message' as action, toDate(time) as day
                    from {db}.message_actions
                    union all
                    select user_id, action, toDate(time) as day
                    from {db}.feed_actions) q1
                    WHERE day > today()-11 AND day < today()
                    group by day, action"""
        
        report_10_days_feed = ph.read_clickhouse(query_5, connection=connection)
        return report_10_days_feed
    
    @task
    # Загружаем данные о New/retained/gone
    def extract_new_retained_gone():
        query_6 = """SELECT toDate(this_week) as this_week, previous_week, -uniq(user_id) as num_users, status 
        FROM
        (SELECT user_id, 
        groupUniqArray(toMonday(toDate(time))) as weeks_visited, 
        addWeeks(arrayJoin(weeks_visited), +1) this_week, 
        if(has(weeks_visited, this_week) = 1, 'retained', 'gone') as status, 
        addWeeks(this_week, -1) as previous_week
        FROM {db}.feed_actions
        group by user_id)
        where status = 'gone'
        group by this_week, previous_week, status
        HAVING this_week != addWeeks(toMonday(today()), +1)
        union all
        SELECT this_week, previous_week, toInt64(uniq(user_id)) as num_users, status FROM
        (SELECT user_id, 
        groupUniqArray(toMonday(toDate(time))) as weeks_visited, 
        arrayJoin(weeks_visited) this_week, 
        if(has(weeks_visited, addWeeks(this_week, -1)) = 1, 'retained', 'new') as status, 
        addWeeks(this_week, -1) as previous_week
        FROM {db}.feed_actions
        group by user_id)
        group by this_week, previous_week, status
        """
        report_new_retained_gone = ph.read_clickhouse(query_6, connection=connection)
        report_new_retained_gone['this_week'] = report_new_retained_gone['this_week'].astype('str') # чтобы даты на графике были без времени
        return report_new_retained_gone
    
    @task
    # Загружаем данные retention 14 days
    def extract_retention_14_days():
        query_7 = """
    select date_diff(day, day1, time) as retention_day, 
    count(user_id) as count_user
    from (
    select user_id, source, min(time) as day1
    from (
    select DISTINCT user_id, toDate(time) as time, source
    from {db}.feed_actions 
    where toDate(time) <= '2022-11-01') q1
    group by user_id, source) q2
    join
    (select DISTINCT user_id, toDate(time) as time, source
    from simulator_20221020.feed_actions 
    where toDate(time) <= '2022-11-01') q3 using user_id
    where date_diff(day, day1, time) <= 14
    group by date_diff(day, day1, time) 
    order by retention_day"""

        report_retention = ph.read_clickhouse(query_7, connection=connection)
        return report_retention
    
    @task
    # Готовим графики CTR, likes & views & messages, message per users
    def prepare_plot_1(report_10_days_ctr, report_10_days_feed, report_10_days_sms):
        fig, ax = plt.subplots(nrows = 3, ncols = 1, figsize=(20, 10))
        fig.suptitle('Ключевые данные за последние 10 дней', fontsize=20)

        sns.lineplot(ax=ax[0], data=report_10_days_ctr, x='day', y='CTR', color = 'darkmagenta')
        ax[0].set_title('CTR', fontsize=16)
        ax[0].grid()

        sns.lineplot(ax=ax[1], data=report_10_days_feed, x='day', y = 'count_user', hue = 'action')
        ax[1].set_title('likes & views & messages', fontsize=16)
        ax[1].grid()

        sns.lineplot(ax=ax[2], data=report_10_days_sms, x='day', y = 'sms_per_user', color ='DarkSlateGrey')
        ax[2].set_title('message per users', fontsize=16)
        ax[2].grid()

        plot_1 = io.BytesIO()
        plt.savefig(plot_1)
        plot_1.seek(0)
        plot_1.name = 'plot1.png'
        plt.close()

        return plot_1

    
    @task
    # Готовим график New/retained/gone
    def prepare_plot_2(report_new_retained_gone):
        fig, ax = plt.subplots(nrows = 1, ncols = 1, figsize=(20, 10))
        fig.suptitle('New/retained/gone', fontsize=20)
        
        ax.grid()        
        sns.barplot(data=report_new_retained_gone, x='this_week', y='num_users', hue='status', color = 'DarkCyan')

        plot_2 = io.BytesIO()
        plt.savefig(plot_2)
        plot_2.seek(0)
        plot_2.name = 'plot2.png'
        plt.close()

        return plot_2
    
    
    @task
    # Готовим график retention
    def prepare_plot_3(report_retention):
        fig, ax = plt.subplots(nrows = 1, ncols = 1, figsize=(20, 10))
        fig.suptitle('Retention 14 days', fontsize=20)
        
        ax.grid()
        sns.lineplot(data=report_retention, x='retention_day', y='count_user', color = 'darkmagenta')

        plot_3 = io.BytesIO()
        plt.savefig(plot_3)
        plot_3.seek(0)
        plot_2.name = 'plot3.png'
        plt.close()

        return plot_3

    
    @task
    # Отправляем сообщение в тг
    def send_to_telegram(message, plot_1, plot_2, plot_3):
        bot.sendMessage(chat_id=chat_id, text=message)
        bot.sendPhoto(chat_id=chat_id, photo=plot_1)
        bot.sendPhoto(chat_id=chat_id, photo=plot_2)
        bot.sendPhoto(chat_id=chat_id, photo=plot_3)
    


    report_yesterday = extract_yesterday()
    report_yesterday_sms = extract_yesterday_sms()
    message = prepare_message(report_yesterday, report_yesterday_sms)
    report_10_days_ctr = extract_ctr_10_days()
    report_10_days_sms = extract_sms_per_user_10_days()
    report_10_days_feed = extract_feed_10_days()
    report_new_retained_gone = extract_new_retained_gone()
    report_retention = extract_retention_14_days()
    plot_1 = prepare_plot_1(report_10_days_ctr, report_10_days_feed, report_10_days_sms)
    plot_2 = prepare_plot_2(report_new_retained_gone)
    plot_3 = prepare_plot_3(report_retention)
    send_to_telegram(message, plot_1, plot_2, plot_3)    
        
full_report_telegram_kaminskaya = full_report_telegram_kaminskaya()