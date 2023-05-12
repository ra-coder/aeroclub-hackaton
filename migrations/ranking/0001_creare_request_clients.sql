create table client_requests (
  id serial,
  RequestID int,
  RequestDate timestamp,
  ClientID int,
  ClientGrade int,
  SearchRoute text,
  RequestDepartureDate timestamp,
  RequestReturnDate timestamp,
  FligtOption text,
  DepartureDate timestamp,
  ArrivaDate timestamp,
  ReturnDepatrureDate timestamp,
  ReturnArrivalDate timestamp,
  SegmentCount int,
  Amount numeric(16, 2),
  "class" varchar,
  IsBaggage bool,
  isRefundPermitted bool,
  isExchangePermitted bool,
  isDiscount bool,
  InTravelPolicy bool,
  FrequentFlyer bool,
  SelectedVariant bool
);


-- RequestID	"Запрос на поиск"	int	ID запроса
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
-- FrequentFlyer	характеристика конкретного путешественника	varchar	наличие бонусной карты у путешественника
-- SelectedVariant		bit	Вариант добавлен в Offer
--
