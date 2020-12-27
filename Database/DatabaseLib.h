#pragma once
#pragma warning (disable : 4251)
#pragma warning (disable: 4275)

#ifdef DATABASE_EXPORTS
#define DATABASE_API __declspec(dllexport)
#else
#define DATABASE_API __declspec(dllimport)
#endif