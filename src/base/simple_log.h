/*
 * simple_log.h
 *
 *  Created on: 2016-3-14
 *      Author: ziteng
 */

#ifndef __BASE_SIMPLE_LOG_H__
#define __BASE_SIMPLE_LOG_H__

#include <stdio.h>
#include <stdarg.h>
#include <string>
#include <mutex>
using std::string;
using std::mutex;

enum LogLevel {
    kLogLevelError      = 0,
    kLogLevelWarning    = 1,
    kLogLevelInfo       = 2,
    kLogLevelDebug      = 3,
};

const string kLogLevelName[] = {"[ERROR]", "[WARNING]", "[INFO]", "[DEBUG]"};

const int kMaxLogLineSize = 4096;

class SimpleLog {
public:
    SimpleLog();
    virtual ~SimpleLog();
    
    void Init(LogLevel level, const string& path);
    void LogMessage(LogLevel level, const char* fmt, ...);
    void SetLogLevel(LogLevel level) { log_level_ = level; }
private:
    void OpenFile();
    bool IsNewDay(time_t current_time);
private:
    LogLevel    log_level_;
    string      log_path_;
    FILE*       log_file_;
    time_t      last_log_time_;
    char        log_buf_[kMaxLogLineSize];
    mutex       mutex_;
};

// if application only has one log path, use these two api to simplify the log process
// see http://gcc.gnu.org/onlinedocs/cpp/Variadic-Macros.html for explanation of ##__VA_ARGS__
extern SimpleLog g_simple_log;
void init_simple_log(LogLevel level, const string& path);
#define log_message(level, fmt, ...) g_simple_log.LogMessage(level, fmt, ##__VA_ARGS__)

#endif
