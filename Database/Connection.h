#pragma once
#include "DatabaseLib.h"
#include <atomic>

namespace DatabaseLib
{
	class DATABASE_API Connection
	{
	private:
		static std::atomic<unsigned int> currentId;
		unsigned connectionId;
	public:
		unsigned getConnectionId() const;
		Connection();
	};
}