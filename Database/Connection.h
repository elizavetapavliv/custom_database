#pragma once
#include "DatabaseLib.h"
#include <atomic>

namespace DatabaseLib
{
	class DATABASE_API Connection
	{
	private:
		static std::atomic<unsigned int> currentId;
	public:
		unsigned connectionId;
		Connection();
		bool operator==(const Connection& other) const;
	};
}