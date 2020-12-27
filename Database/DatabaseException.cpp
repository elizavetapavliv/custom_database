#include "pch.h"
#include "DatabaseException.h"

namespace DatabaseLib
{
	DatabaseException::DatabaseException(std::string message, ErrorCode errorCode) 
        : message(message), errorCode(errorCode) {}

    const char* DatabaseException::what() const throw ()
    {
        return message.c_str();
    }

    ErrorCode DatabaseException::getErrorNumber() const throw() {
        return errorCode;
    }

}