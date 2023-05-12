create table agent_requests
(
    id                   serial,
    RequestID            int,
    EmployeeId           int,
    RequestDate          timestamp,
    ClientID             int,
    TravellerGrade       int,
    SearchRoute          text,
    RequestDepartureDate timestamp,
    RequestReturnDate    timestamp,
    FligtOption          text,
    DepartureDate        timestamp,
    ArrivalDate          timestamp,
    ReturnDepatrureDate  timestamp,
    ReturnArrivalDate    timestamp,
    SegmentCount         int,
    Amount               numeric(16, 2),
    class                varchar,
    IsBaggage            bool,
    isRefundPermitted    bool,
    isExchangePermitted  bool,
    isDiscount           bool,
    InTravelPolicy       bool,
    SentOption           bool
)

-- Столбец	часть данных	Тип данных	что это
-- RequestID	"Запрос
-- на поиск"	int	ID запроса
-- EmployeeId		int	ID операционного сотрудника
-- RequestDate		Datetime	Дата запроса
-- ClientID		int	ID Клиента
-- TravellerGrade		varchar	Уровень путешественника
-- SearchRoute		varchar	Маршрут
-- RequestDepartureDate		Datetime	дата вылета
-- RequestReturnDate		Datetime	дата обратного вылета
-- FligtOption		varchar	вариант перелета
-- DepartureDate		Datetime	дата + время вылета в точку назначения
-- ArrivalDate		Datetime	дата + время прилета в точку назначения
-- ReturnDepatrureDate		Datetime	дата + время обратного вылета
-- ReturnArrivalDate		Datetime	дата + время обратного прилета
-- SegmentCount		int	количество перелетов по варианту
-- Amount		decimal	стоимость варианта
-- class		varchar(1)	класс перевозки
-- IsBaggage		bit	наличие бесплатного багажа
-- isRefundPermitted		bit	возможность обмена
-- isExchangePermitted		bit	возможность возврата
-- isDiscount		bit	стоимость со скидкой
-- InTravelPolicy		bit	разрешено использовать по политике клиента
-- SelectedVariant		bit	Вариант добавлен в Offer