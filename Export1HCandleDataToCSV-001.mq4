//+------------------------------------------------------------------+
//|                                       ExportCandleDataToCSV2.mq4 |
//|                                               Maximiliano Lucius |
//|                                                             None |
//+------------------------------------------------------------------+
#property copyright "Maximiliano Lucius"
#property link      "None"
#property version   "1.00"
#property strict
//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+

int fileHandle;
datetime lastCandleTime;


int OnInit(){
//---
    datetime currentTime = TimeGMT();
    
    // Convert current time to string with desired precision
    MqlDateTime str1;
    TimeToStruct(currentTime, str1);
    string dateString = StringFormat("%04d-%02d-%02d", str1.year, str1.mon, str1.day);

    // Generate the filename with the current date
    string fileName = _Symbol + "_" + dateString + "-1H.csv";
    fileHandle = FileOpen(fileName, FILE_READ|FILE_WRITE|FILE_TXT|FILE_ANSI);    
    if(fileHandle != INVALID_HANDLE){
        // File opened successfully
        // You can perform any additional initialization tasks here if needed
        FileSeek(fileHandle, 0, SEEK_END); // Move to the end of the file
        FileWrite(fileHandle, "Date,Open, High,Low,Close,Volume");       
        Print("We are ready for ... ", _Symbol);
    } else {
        // Failed to open file
        Print("Failed to open file for writing: ", fileName);
    }
    lastCandleTime = iTime(_Symbol, PERIOD_D1, 1);

    return INIT_SUCCEEDED;
}
//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason){
//---
    FileClose(fileHandle);
    Print("Tha's all!");
}
//+------------------------------------------------------------------+
//| Expert tick function                                             |
//+------------------------------------------------------------------+
void OnTick(){
//---
    double openPrice = 0;
    double highPrice = 0;
    double lowPrice = 0;
    double closePrice = 0;
    double volume = 0;
    // Get current time in UTC 0 TimeMilliseconds
    datetime currCandleTime = iTime(_Symbol, PERIOD_H1, 1);


    // Convert current time to string
    //string timeString = TimeToString(currentTime, TIME_DATE|TIME_MINUTES);
    if(currCandleTime != lastCandleTime){
        if(lastCandleTime != 0){
            MqlDateTime str1;
            TimeToStruct(currCandleTime, str1);
            string timeString = StringFormat("%04d-%02d-%02d %02d:%02d:%02d", str1.year, str1.mon, str1.day, str1.hour, str1.min, str1.sec);
            
            openPrice = iOpen(_Symbol, PERIOD_H1, 1);
            highPrice = iHigh(_Symbol, PERIOD_H1, 1);
            lowPrice = iLow(_Symbol, PERIOD_H1, 1);
            closePrice = iClose(_Symbol, PERIOD_H1, 1);
            volume = (double)iVolume(_Symbol, PERIOD_H1, 1);
            FileWrite(fileHandle, timeString + "," + 
                                DoubleToString(openPrice, _Digits) + "," + 
                                DoubleToString(highPrice, _Digits) + "," + 
                                DoubleToString(lowPrice, _Digits) + "," + 
                                DoubleToString(closePrice, _Digits) + "," + 
                                DoubleToString(volume, 0));
                                
            lastCandleTime = currCandleTime;
        }
    }    
}
//+------------------------------------------------------------------+
