#pragma once
#include "DatabaseLib.h"
#include "ErrorCode.h"
#include <string>

namespace DatabaseLib
{
    class DATABASE_API DatabaseException : public std::exception {
    private:
        std::string message;
        ErrorCode errorCode;
    public:
        DatabaseException(std::string message, ErrorCode errorCode);
        const char* what() const throw ();
        ErrorCode getErrorNumber() const throw();
    };
}