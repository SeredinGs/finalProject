# Финальный проект - Разработка сервиса по выдаче кредита. Раздел с генератором данных
Состоит из следующих компонетов:
- генератор выданных кредитов(gen_hist)
- генератор транзакций(get_transactions)

Для подключения нужно в Postgres создать пользователя, для этого пользователя создать отдельную БД.
Рядом со скриптами положить 2 файла: amounts.txt и transactions.txt, содержащий следующие данные:
- IP-адрес Postgres
- Имя БД
- Имя схемы в БД
- Имя таблицы
- Имя созданного пользователя
- Пароль