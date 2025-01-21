//+------------------------------------------------------------------+
//|                                      ExportTickDataToCSV-001.mq4 |
//|                                  Copyright 2025, MetaQuotes Ltd. |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+

#property copyright "Maximiliano Lucius"
#property link      "None"
#property version   "1.00"
#property strict

// Global variables
int fileHandle;
int currentDay = -1; // Initialize to an invalid value

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
    //---
    currentDay = Day(); // Initialize the current day
    OpenNewFile();
    
    return INIT_SUCCEEDED;
}

//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
    //---
    FileClose(fileHandle);
    Print("That's all!");
}

//+------------------------------------------------------------------+
//| Expert tick function                                             |
//+------------------------------------------------------------------+
void OnTick()
{
    // Check if the day has changed
    if (Day() != currentDay)
    {
        // Close the current file
        FileClose(fileHandle);
        
        // Update the current day
        currentDay = Day();
        
        // Open a new file for the new day
        OpenNewFile();
    }
    
    // Get current time in UTC
    datetime currentTime = TimeGMT();
    
    // Convert current time to string with desired precision
    MqlDateTime str1;
    TimeToStruct(currentTime, str1);
    string timeString = StringFormat("%04d-%02d-%02d %02d:%02d:%06.3f", str1.year, str1.mon, str1.day, str1.hour, str1.min, str1.sec);
    
    // Write the tick data to the file
    FileWrite(fileHandle, timeString + "," + DoubleToString(Ask, _Digits) + "," + DoubleToString(Bid, _Digits));
}

//+------------------------------------------------------------------+
//| Function to open a new file and write the header                 |
//+------------------------------------------------------------------+
void OpenNewFile()
{
    // Get current time in UTC
    datetime currentTime = TimeGMT();
    
    // Convert current time to string with desired precision
    MqlDateTime str1;
    TimeToStruct(currentTime, str1);
    string dateString = StringFormat("%04d-%02d-%02d", str1.year, str1.mon, str1.day);

    // Generate the filename with the current date
    string fileName = _Symbol + "_ticks_" + dateString + ".csv";
    
    // Open the file
    fileHandle = FileOpen(fileName, FILE_READ|FILE_WRITE|FILE_TXT|FILE_ANSI);    
    if (fileHandle != INVALID_HANDLE)
    {
        // File opened successfully
        FileSeek(fileHandle, 0, SEEK_END); // Move to the end of the file
        FileWrite(fileHandle, "datetime,Ask,Bid"); // Write the header
        Print("New file created: ", fileName);
    }
    else
    {
        // Failed to open file
        Print("Failed to open file for writing: ", fileName);
    }
}
//+------------------------------------------------------------------+