//+------------------------------------------------------------------+
//|                                                 ON_Cassandra.mq4 |
//|                                   Translated from Python to MQL4 |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+

/*
TimeCurrent gives the server time in GMT-0. Internally, it is the number of seconds (rounded to the nearest second) since 1970-01-01.
The Expert Advisor will work with internal time in GMT-0. (Any time will be converted to GMT-0).
*/

#property copyright "Viridis"    // Author/owner
#property link      "hey.com"    // Contact/website
#property version   "1.00"       // EA version
#property strict

//+------------------------------------------------------------------+
#property strict

// Input parameters
input int MagicNumber = 202501171;              // Unique EA identifier
input string comment = "ON Cassandra";          // Order comment
input double LotSize = 1.0;                     // Order volume
input bool ContinueTrading = true;              // Enable/disable trading

input int OpenTimeHour = 17;                    // Order entry start hour (NY)
input int OpenTimeMinute = 0;                   // Order entry start minute
input int CloseTimeHour = 10;                   // Order entry close hour (NY)
input int CloseTimeMinute = 00;                 // Order entry close minute
input int TimeToleranceWindow = 1800;           // Order acceptance window [s]

input bool Verbose = false;                     // Enable detailed logging
input double nbi = 3594.52;                     // Fibonacci base price
input double nti = 4808.93;                     // Fibonacci top price
input int Zin = 14;                             // Initial state value
input string Vstr = "3,5,6,7,9";                // Inhibit state list.



// Global variables
int Z;
int V[];             // New parameter V (vector of inhibitors)
datetime BuyTime;                              // Time to buy
datetime SellTime;                             // Time to sell
datetime openTimeGMT0Lb, openTimeGMT0Ub, closeTimeGMT0Lb, closeTimeGMT0Ub;
MqlDateTime timeStruct;

bool PositionOpen = false;                     // Track if a position is open
int CurrentTicket = -1;                        // Ticket of the current open order
datetime LastTradingTimesUpdate;               // Last time trading times were set
string fiboPrefix = "FiboLine_"; // Prefix for Fibonacci level lines

// Global variables for Fibonacci levels
double fibo_base = 0.0;
double fibo_38 = 0.0;
double fibo_50 = 0.0;
double fibo_61 = 0.0;
double fibo_top = 0.0;
double fibo_138 = 0.0;
double fibo_150 = 0.0;
double fibo_161 = 0.0;
double fibo_200 = 0.0;


// Previous reference price
double prev_ref_price = 0.0;
double _prev_ref_price = 0.0;

// Internal state variables
datetime last_candle_time;           // Last candle time
datetime open_date;                  // Date on which we open a position


// Session candle zone variables
int prev_session_last_candle_zone = Z;      // Previous session last candle zone
int current_session_last_candle_zone = Z;   // Current session last candle zone


// Function to parse the input string and populate the array
void ParseInputString(string _input, int &arr[])
{
    // Check if input is empty
    if (StringLen(_input) == 0){
        ArrayResize(arr, 0); // Resize to zero if input is empty
        return;
    }

    // Split the input string by commas
    string parts[];
    int count = StringSplit(_input, ',', parts);

    // Resize the array to match the number of elements
    ArrayResize(arr, count);

    // Populate the array with the parsed values
    for (int i = 0; i < count; i++){
        arr[i] = int(StringToInteger(parts[i])); // Convert each part to integer
        // Optionally: Add error handling for conversion if necessary
    }
}

// Function to check if Z is in V
bool IsZInV(int z, int &arr[]){
    if (ArraySize(arr) == 0) 
        return false; // If V is empty, Z is not in V

    for (int i = 0; i < ArraySize(arr); i++) {
        if (arr[i] == z) {
            return true; // Z is found in V
        }
    }
    return false; // Z is not in V
}

// Function to calculate Fibonacci levels
void CalculateFibonacciLevels()
{
    // Ensure nti and nbi are valid
    if (nti <= nbi)
    {
        Print("Error: 'nti' must be greater than 'nbi' for valid Fibonacci calculation.");
        return;
    }

    double size_ = nti - nbi;
    fibo_base = nbi;
    fibo_38 = nbi + 0.382 * size_;
    fibo_50 = nbi + 0.50 * size_;
    fibo_61 = nbi + 0.618 * size_;
    fibo_top = nti;
    fibo_138 = nbi + 1.382 * size_;
    fibo_150 = nbi + 1.50 * size_;
    fibo_161 = nbi + 1.618 * size_;
    fibo_200 = nbi + 2.0 * size_;

    // Draw lines for each Fibonacci level
    DrawFibonacciLine(fiboPrefix + "Base", fibo_base);
    DrawFibonacciLine(fiboPrefix + "38.2", fibo_38);
    DrawFibonacciLine(fiboPrefix + "50.0", fibo_50);
    DrawFibonacciLine(fiboPrefix + "61.8", fibo_61);
    DrawFibonacciLine(fiboPrefix + "Top", fibo_top);
    DrawFibonacciLine(fiboPrefix + "138.2", fibo_138);
    DrawFibonacciLine(fiboPrefix + "150.0", fibo_150);
    DrawFibonacciLine(fiboPrefix + "161.8", fibo_161);
    DrawFibonacciLine(fiboPrefix + "200.0", fibo_200);

    // Log Fibonacci levels if verbose is enabled
    if (Verbose)
    {
        Print("Calculated Fibonacci Levels:");
        PrintFormat(" Base:   %.2f", fibo_base);
        PrintFormat(" 38.2%%:  %.2f", fibo_38);
        PrintFormat(" 50.0%%:  %.2f", fibo_50);
        PrintFormat(" 61.8%%:  %.2f", fibo_61);
        PrintFormat(" Top:    %.2f", fibo_top);
        PrintFormat("138.2%%:  %.2f", fibo_138);
        PrintFormat("150.0%%:  %.2f", fibo_150);
        PrintFormat("161.8%%:  %.2f", fibo_161);
        PrintFormat("200.0%%:  %.2f", fibo_200);
    }
}

// Function to draw a horizontal line at a specific price level
void DrawFibonacciLine(string objectName, double priceLevel)
{
    if (ObjectFind(0, objectName) == -1)
    {
        ObjectCreate(0, objectName, OBJ_HLINE, 0, 0, priceLevel);
    }

    // Set line properties
    ObjectSetInteger(0, objectName, OBJPROP_COLOR, clrLightBlue); // Light blue color
    ObjectSetInteger(0, objectName, OBJPROP_STYLE, STYLE_DOT);    // Dotted line
    ObjectSetInteger(0, objectName, OBJPROP_WIDTH, 1);           // Line thickness
    ObjectSetDouble(0, objectName, OBJPROP_PRICE1, priceLevel);  // Set price level
}

// Function to delete all Fibonacci lines
void DeleteFibonacciLines()
{
    int totalObjects = ObjectsTotal(0, -1, -1); // Specify chart_id, sub_window, and type
    for (int i = totalObjects - 1; i >= 0; i--)
    {
        string objName = ObjectName(0, i);
        if (StringFind(objName, fiboPrefix) == 0) // Check for prefix
        {
            ObjectDelete(0, objName);
        }
    }
}


// Function to update the state based on reference price
void UpdateState(double ref_price){
    // Check conditions and update Z based on Fibonacci levels
    if (ref_price < fibo_base && _prev_ref_price >= fibo_base)
        Z = 1;
    else if (ref_price < fibo_38 && _prev_ref_price >= fibo_38)
        Z = 3;
    else if (ref_price < fibo_50 && _prev_ref_price >= fibo_50)
        Z = 5;
    else if (ref_price < fibo_61 && _prev_ref_price >= fibo_61)
        Z = 7;
    else if (ref_price < fibo_top && _prev_ref_price >= fibo_top)
        Z = 9;
    else if (ref_price < fibo_138 && _prev_ref_price >= fibo_138)
        Z = 11;
    else if (ref_price < fibo_161 && _prev_ref_price >= fibo_161)
        Z = 13;
    else if (ref_price < fibo_200 && _prev_ref_price >= fibo_200)
        Z = 15;

    if (ref_price > fibo_200 && _prev_ref_price <= fibo_200)
        Z = 16;
    else if (ref_price > fibo_161 && _prev_ref_price <= fibo_161)
        Z = 14;
    else if (ref_price > fibo_138 && _prev_ref_price <= fibo_138)
        Z = 12;
    else if (ref_price > fibo_top && _prev_ref_price <= fibo_top)
        Z = 10;
    else if (ref_price > fibo_61 && _prev_ref_price <= fibo_61)
        Z = 8;
    else if (ref_price > fibo_50 && _prev_ref_price <= fibo_50)
        Z = 6;
    else if (ref_price > fibo_38 && _prev_ref_price <= fibo_38)
        Z = 4;
    else if (ref_price > fibo_base && _prev_ref_price <= fibo_base)
        Z = 2;

    // Verbose logging if enabled
    if (Verbose){
        Print("Reference Price: ", ref_price, "   Previous Hourly Open Price: ", _prev_ref_price, "   Updated State Z: ", Z);
    }

    // Update previous reference price
    _prev_ref_price = ref_price;
}

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
    Z = Zin;
    
    ParseInputString(Vstr, V);

    // Initialize trading times
    SetTradingTimes();
    CalculateFibonacciLevels();
    LastTradingTimesUpdate = TimeCurrent();
    // Set the timer to trigger every 60 minutes (3600 seconds)
    
    // Check for open orders by symbol and magic number
    for (int i = 0; i < OrdersTotal(); i++){
        if (OrderSelect(i, SELECT_BY_POS, MODE_TRADES)){
            if (OrderSymbol() == Symbol() && OrderMagicNumber() == MagicNumber){
                PositionOpen = true;
                CurrentTicket = OrderTicket();
                if (Verbose) {
                    Print("Found open order with Ticket: ", CurrentTicket);
                }
                break;
            }
        }
    }
        
    Print("# ---- Cassandra ON ready!! ----");
    
    return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
    // Cleanup (if needed)
    DeleteFibonacciLines();
}

//+------------------------------------------------------------------+
//| Expert tick function                                             |
//+------------------------------------------------------------------+
void OnTick(){

    // Get the current time
    datetime CurrentTime = TimeCurrent(); // Last server time. It is seems MQL4 does not manage GMT offset.
    
    // Check if it's time to buy or sell ----------------------------------
    if (CurrentTime >= openTimeGMT0Lb && CurrentTime < openTimeGMT0Ub && !PositionOpen && !IsZInV(Z, V) && ContinueTrading){
        ExecuteOrder("BUY", Bid);
    } else if (CurrentTime >= closeTimeGMT0Lb && PositionOpen){ //  && CurrentTime < closeTimeGMT0Ub
        ExecuteOrder("STOP", Bid);
        SetTradingTimes();
    }
    // EOB Check if it's time to buy or sell ----------------------------------


   
    // ----  Next block update Fibonacci zones in a hourly basis.
    datetime current_candle_time = iTime(NULL, PERIOD_H1, 0);

    // Check if a new hourly candle has closed
    // Check if an hour has passed since the last update of trading times
    if (current_candle_time != last_candle_time)
    {
        double close_price = iClose(NULL, PERIOD_H1, 1); // Get the close price of the previous candle
        UpdateState(close_price);
        
        UpdateFiboZone(close_price);

        if ((timeStruct.day_of_week == 0 || timeStruct.day_of_week == 6) || !PositionOpen){ // 0=Sunday, 6=Saturday
            SetTradingTimes();
        }
        
        last_candle_time = current_candle_time;
    }    
}

void UpdateFiboZone(const double close_price){
    // Estoy a las 16:00 de NY Times ??
    // Get the current time in GMT
    datetime CurrentTimeGMT = TimeGMT();

    // Convert GMT time to New York time (considering DST)
    int ny_offset = 5; // Standard offset from GMT to New York (EST)
    if (IsNewYorkDST(CurrentTimeGMT)) {
        ny_offset = 4; // EDT (DST) offset
    }
    datetime NYTime = CurrentTimeGMT - ny_offset * 3600;
    // Calculate buy and sell times based on New York time
    datetime todayNY = StrToTime(IntegerToString(TimeYear(NYTime)) + "." + IntegerToString(TimeMonth(NYTime)) + "." + IntegerToString(TimeDay(NYTime)));
    datetime FiboCriticalTimeInNYZone = todayNY + 16 * 3600; // 16:00 New York time

    last_candle_time = iTime(NULL, PERIOD_H1, 1);
    datetime last_candle_time_in_NYTime = last_candle_time - ny_offset * 3600;


    if (last_candle_time_in_NYTime == FiboCriticalTimeInNYZone){
        prev_session_last_candle_zone = current_session_last_candle_zone;
        current_session_last_candle_zone = Z;
        if (Verbose){
            // print(f'ts: {ts}  open: {candle["open"]}  prev_session_last_candle_zone: {self.prev_session_last_candle_zone}  current_session_last_candle_zone: {self.current_session_last_candle_zone}')
            Print("Write log here."); // Missing work here.
        }
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
    closeTimeGMT0Lb = todayNY + (CloseTimeHour * 3600 + CloseTimeMinute * 60) + broker_to_ny_offset * 3600 + 24 * 60 * 60;  // Cierra al dia siguiente.
    closeTimeGMT0Ub = openTimeGMT0Lb + TimeToleranceWindow;
    
    
    TimeToStruct(openTimeGMT0Lb, timeStruct); // Convert datetime to MqlDateTime structure

    if (Verbose) {
        Print("[TDBot] Buy Time: ", TimeToString(BuyTime), ", Sell Time: ", TimeToString(SellTime));
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
            PositionOpen = true;
        } else {
            Print("Error placing BUY order: ", GetLastError());
        }
    }
    else if (OrderType == "STOP" && CurrentTicket > 0)
    {
        // Close the current order
        if (OrderClose(CurrentTicket, LotSize, Bid, 3, Red)){
            Print(TimeToString(TimeCurrent()), " - Executing CLOSE order: Ticket=", CurrentTicket);
            PositionOpen = false;
        } else {
            Print("Error closing order: ", GetLastError());
        }
    }
}