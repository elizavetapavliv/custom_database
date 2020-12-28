#pragma once
#include "DatabaseLib.h"

namespace DatabaseLib 
{
	enum class DATABASE_API ErrorCode
	{
		NO_CONNECTION = 1,
		NOT_FOUND,
		TABLE_NOT_FOUND,
		KEY_NOT_FOUND,
		KEY_VALUE_NOT_FOUND,
		CURSOR_NOT_OPENED,
		NO_MORE_DATA_AVAILABLE
	};
}