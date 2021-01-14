#pragma once
#include "nlohmann/json.hpp"

namespace DatabaseLib
{
	using json = nlohmann::json;

    struct JsonComparator {
        bool operator() (const json& a, const json& b) const {
			auto itA = a.begin();
			auto itB = b.begin();

			while (itA != a.end() && itB != b.end())
			{
				if (itA.value() < itB.value()) 
				{
					return true;
				}
				else if (itA.value() > itB.value())
				{
					return false;
				}
				itA++;
				itB++;
			}
			return false;
        }
    };
}
