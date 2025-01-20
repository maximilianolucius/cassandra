//+------------------------------------------------------------------+
//|                                                 TD_Cassandra.mq4 |
//|                        Translated from Python to MQL4            |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+

/*
TimeCurrent gives the server time in GMT-0. Internally, it is the number of seconds (rounded to the nearest second) since 1970-01-01.
The Expert Advisor will work with internal time in GMT-0. (Any time will be converted to GMT-0).
*/

#property copyright "Viridis"
#property link      "viridis.com"
#property version   "1.00"
#property strict

//+------------------------------------------------------------------+
#property strict

// Input parameters
input int MagicNumber = 20250120;              //Magic Number
input string comment = "TD Cassandra";
input double LotSize = 1.0;                    // Lot size for trading

/*
Con el precio de cierre de la candela horario de las 10, abre orden de compra: es congruente con decir que a las 11:00 NY time opera.
*/
input int OpenTimeHour = 11;                    // New York Open Order Hour 
input int OpenTimeMinute = 0;                    // New York Open Order Minutes
input int CloseTimeHour = 16;                    // New York Open Order Hour
input int CloseTimeMinute = 00;                    // New York Open Order Minutes
input int TimeToleranceWindow = 1800;             //  Acceptance windows in seconds

input bool Verbose = false;                     // Enable verbose logging


// Global variables
datetime BuyTime;                              // Time to buy
datetime SellTime;                             // Time to sell
bool PositionOpened = false;                     // Track if a position is open
int CurrentTicket = -1;                        // Ticket of the current open order
datetime LastTradingTimesUpdate;               // Last time trading times were set
datetime openTimeGMT0Lb, openTimeGMT0Ub, closeTimeGMT0Lb, closeTimeGMT0Ub;


//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit(){

    EventSetTimer(3600);

    // Initialize trading times
    SetTradingTimes();
    LastTradingTimesUpdate = TimeCurrent();
    // Set the timer to trigger every 60 minutes (3600 seconds)
    
    // Check for open orders by symbol and magic number
    for (int i = 0; i < OrdersTotal(); i++){
        if (OrderSelect(i, SELECT_BY_POS, MODE_TRADES)){
            if (OrderSymbol() == Symbol() && OrderMagicNumber() == MagicNumber){
                PositionOpened = true;
                CurrentTicket = OrderTicket();
                if (Verbose) {
                    Print("Found open order with Ticket: ", CurrentTicket);
                }
                break;
            }
        }
    }
        
    Print("# ---- Cassandra TD ready!! ----");
    
    return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
//| Timer event handler                                              |
//+------------------------------------------------------------------+
void OnTimer(){
    // Update trading times
    Print("OnTimer()");
    
    SetTradingTimes();
}

//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
    // Cleanup (if needed)
}

//+------------------------------------------------------------------+
//| Expert tick function                                             |
//+------------------------------------------------------------------+
void OnTick(){

    // Get the current time
    datetime CurrentTime = TimeCurrent(); // Last server time. It is seems MQL4 does not manage GMT offset.
    
    // Check if an hour has passed since the last update of trading times
    if ( int(CurrentTime - LastTradingTimesUpdate) >= 3600 ){
        SetTradingTimes();
        LastTradingTimesUpdate = CurrentTime;
    }
    
    // Check if it's time to buy or sell
    if (CurrentTime >= openTimeGMT0Lb && CurrentTime < openTimeGMT0Ub && !PositionOpened){
        ExecuteOrder("BUY", Bid);
    } else if (CurrentTime >= closeTimeGMT0Lb && PositionOpened){ //  && CurrentTime < closeTimeGMT0Ub 
        ExecuteOrder("STOP", Bid);
        Print("[TDBot] Buy Time: ", TimeToString(openTimeGMT0Lb), ", Sell Time: ", TimeToString(closeTimeGMT0Lb));
    }
}

//+------------------------------------------------------------------+
//| Set trading times based on market timezone                       |
//+------------------------------------------------------------------+
void SetTradingTimes()
{
    // Get the current time in GMT
    datetime CurrentTimeGMT = TimeGMT();

    // Convert GMT time to New York time (considering DST)
    datetime NYTime = iTime(NULL, PERIOD_D1, 0); // Get the current time on the chart (in broker's timezone)
    int broker_to_ny_offset = 5; // Standard offset from GMT to New York (EST)
    if (IsNewYorkDST(CurrentTimeGMT)) {
        broker_to_ny_offset = 4; // EDT (DST) offset
    }
    NYTime = CurrentTimeGMT - broker_to_ny_offset * 3600;

    // Calculate buy and sell times based on New York time
    datetime todayNY = StrToTime(IntegerToString(TimeYear(NYTime)) + "." + IntegerToString(TimeMonth(NYTime)) + "." + IntegerToString(TimeDay(NYTime)));
    BuyTime = todayNY + 10 * 3600; // 10:00 AM New York time
    SellTime = todayNY + 15 * 3600 - 5 * 60; // 14:55 PM New York time

    openTimeGMT0Lb = todayNY + (OpenTimeHour * 3600 + OpenTimeMinute * 60) + broker_to_ny_offset * 3600;
    openTimeGMT0Ub = openTimeGMT0Lb + TimeToleranceWindow;
    closeTimeGMT0Lb = todayNY + (CloseTimeHour * 3600 + CloseTimeMinute * 60) + broker_to_ny_offset * 3600;
    closeTimeGMT0Ub = openTimeGMT0Lb + TimeToleranceWindow;

    Print("[TDBot] Buy Time: ", TimeToString(openTimeGMT0Lb), ", Sell Time: ", TimeToString(closeTimeGMT0Lb), " broker_to_ny_offset:", broker_to_ny_offset);

    if (Verbose) {
        Print("[TDBot] Buy Time: ", TimeToString(openTimeGMT0Lb), ", Sell Time: ", TimeToString(closeTimeGMT0Lb), " broker_to_ny_offset:", broker_to_ny_offset);
    }
}

//+------------------------------------------------------------------+
//| Check if New York is currently in Daylight Saving Time           |
//+------------------------------------------------------------------+
bool IsNewYorkDST(datetime CurrentTimeGMT)
{
    int year = TimeYear(CurrentTimeGMT);
    int month = TimeMonth(CurrentTimeGMT);
    int day = TimeDay(CurrentTimeGMT);

    // Determine the start and end dates for DST in New York
    datetime dstStart = GetDSTStartDate(year);
    datetime dstEnd = GetDSTEndDate(year);

    // Check if the current date is within the DST period
    return (CurrentTimeGMT >= dstStart && CurrentTimeGMT < dstEnd);
}

//+------------------------------------------------------------------+
//| Get the start date of DST in New York for a given year           |
//+------------------------------------------------------------------+
datetime GetDSTStartDate(int year)
{
    // DST starts on the second Sunday in March
    datetime marchFirst = StrToTime(IntegerToString(year) + ".03.01");
    int dayOfWeek = TimeDayOfWeek(marchFirst);
    int daysToAdd = (dayOfWeek == 0) ? 7 : dayOfWeek; // Adjust for Sunday being 0
    return marchFirst + (14 - daysToAdd) * 86400; // 14 - dayOfWeek gives the number of days to the second Sunday
}

//+------------------------------------------------------------------+
//| Get the end date of DST in New York for a given year             |
//+------------------------------------------------------------------+
datetime GetDSTEndDate(int year)
{
    // DST ends on the first Sunday in November
    datetime novFirst = StrToTime(IntegerToString(year) + ".11.01");
    int dayOfWeek = TimeDayOfWeek(novFirst);
    int daysToAdd = (dayOfWeek == 0) ? 7 : dayOfWeek; // Adjust for Sunday being 0
    return novFirst + (7 - daysToAdd) * 86400; // 7 - dayOfWeek gives the number of days to the first Sunday
}
//+------------------------------------------------------------------+
//| Execute a buy or sell order                                      |
//+------------------------------------------------------------------+
void ExecuteOrder(string OrderType, double Price)
{
    if (OrderType == "BUY")
    {
        // Place a buy order
        CurrentTicket = OrderSend(Symbol(), OP_BUY, LotSize, Ask, 3, 0, 0, comment, MagicNumber, 0, Green);
        if (CurrentTicket > 0){
            Print(TimeToString(TimeCurrent()), " - Executing BUY order: Ticket=", CurrentTicket);
            PositionOpened = true;
        } else {
            Print("Error placing BUY order: ", GetLastError());
        }
    }
    else if (OrderType == "STOP" && CurrentTicket > 0)
    {
        // Close the current order
        if (OrderClose(CurrentTicket, LotSize, Bid, 3, Red)){
            Print(TimeToString(TimeCurrent()), " - Executing CLOSE order: Ticket=", CurrentTicket);
            PositionOpened = false;
        } else {
            Print("Error closing order: ", GetLastError());
        }
    }
}