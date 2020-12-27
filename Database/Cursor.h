#pragma once

namespace DatabaseLib
{
	using Indexes = std::map<json, std::vector<unsigned>, JsonComparator>;
	struct Cursor
	{
		Indexes::iterator currentRow{};
		Indexes::iterator end{};

		int offsetIndex = -1;

		Cursor(Indexes::iterator newCurrentRow, Indexes::iterator end,
			int offsetIndex) : currentRow(newCurrentRow), end(end), offsetIndex(offsetIndex)
		{}

		Cursor() {}
	};
}