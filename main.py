from config import config
from DBManager import DBManager
from utils import *

def main():
    db_name = 'coursework5'
    params = config()
    conn = None
    companies = {5153678: 'ООО ТК Стройрем',
                 14809: 'ООО Бизнес Технологии',
                 164517: 'Metrika',
                 4653155: 'ООО Некст Левел',
                 1485786: 'ООО ИСЭЙ)',
                 2806465: 'Электронная торговая площадка Торги223',
                 85948: 'Aftermarket-DATA',
                 84359: 'ООО Центр-СБК',
                 9624530: 'ООО Р-Софт',
                 13598: 'Ассоциация Коммуникационных Агентств России'}

    vacancies = get_vacancies(companies)
    create_database(db_name, params)
    print(f"БД {db_name} успешно создана")
    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                create_companies_table(cur)
                print("Таблица Companies успешно создана")
                create_vacancies_table(cur)
                print("Таблица Vacancies успешно создана")
                insert_companies_data(cur, companies)
                print("Таблица Companies успешно заполнена")
                insert_vacancies_data(cur, vacancies)
                print("Таблица Vacancies успешно заполнена")
                add_foreign_keys(cur)
                print("Связывание таблиц прошло успешно")
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    db_manager = DBManager(params)
    # db_manager.get_companies_and_vacancies_count()
    # db_manager.get_all_vacancies()
    # db_manager.get_avg_salary()
    # db_manager.get_vacancies_with_higher_salary()
    # db_manager.get_vacancies_with_keyword('Разработчик')
    db_manager.close_connection()

if __name__ == '__main__':
    main()
