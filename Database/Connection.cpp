#include "pch.h"
#include "Connection.h"
#include <atomic>

namespace DatabaseLib
{
	std::atomic<unsigned> Connection :: currentId = 1;

	Connection:: Connection()
	{
		connectionId = currentId++;
	}

	bool Connection:: operator==(const Connection& other) const
	{
		return connectionId == other.connectionId;
	}
}