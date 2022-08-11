# Проект 4

### Описание Задачи
Построить витрину которая будет содержать инфорацию о выплате курьерам

Состав витрины:
- id — идентификатор записи.
- courier_id — ID курьера, которому перечисляем.
- courier_name — Ф. И. О. курьера.
- settlement_year — год отчёта.
- settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь.
- orders_count — количество заказов за период (месяц).
- orders_total_sum — общая стоимость заказов.
- rate_avg — средний рейтинг курьера по оценкам пользователей.
- order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.
- courier_order_sum — сумма, которую необходимо перечислить ресторану. Высчитывается как сумма начислений по заказам, доставленным курьером. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
- courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых.
- courier_reward_sum — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).

PS Правила расчёта процента выплаты курьеру в зависимости от рейтинга, где r — это средний рейтинг курьера в текущем месяце:
- r < 4 — 5% от заказа, но не менее 100 руб.;
- 4 <= r < 4.5 — 7% от заказа, но не менее 150 руб.;
- 4.5 <= r < 4.9 — 8% от заказа, но не менее 175 руб.;
- 4.9 <= r — 10% от заказа, но не менее 200 руб.

### Сбор требований:
- Хранить историю данных с источника AS IS (пер. «как есть»). В этом случае вероятность ошибки при преобразовании данных уменьшится и можно будет в любой момент времени точно оценить, какие данные были в источнике.
- Модель данных «Снежинка» в слое DDS. «Снежинка» позволит быстрее и удобнее формировать новые витрины и соответствующие отчёты.
- Стабильность при изменении формата данных в источниках. Процессы ETL не должны портить данные, если формат данных в источниках изменится.
- Процессы ETL не должны портить данные, если источник не будет доступен.

### Анализ требований: Данные необходимые для реализация витрины можно получить по api

### План реализации:
- Проектирование хранилищ по слоям (STG DDS CDM)
- Выбор модели Данных STG - отсутсвие (as is), dds - Снежинка
- Реализация DWH - порядок загрузки данных STG >> DDS >> CDM
