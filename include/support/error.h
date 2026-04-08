/* (What is this? — Модуль ошибок) */

#pragma once

#include <cstdint>
#include <exception>
#include <filesystem>
#include <string>
#include <string_view>

class Error final : public std::exception {
   public:
    enum class Code : uint16_t {
        InvalidArgument = 1,
        InvalidData = 2,
        NotFound = 3,
        OutOfRange = 4,
        Mismatch = 5,
        Overflow = 6,
        InvalidState = 7,
        Unsupported = 8,
        IoFailure = 9,
    };

    enum class Type : uint8_t {
        Validation,
        Data,
        State,
        Io,
    };

   public:
    Error(Code code, std::string module, std::string module_instance, Type type, std::string message);
    Error(const Error&) = default;
    Error(Error&&) noexcept = default;
    Error& operator=(const Error&) = default;
    Error& operator=(Error&&) noexcept = default;
    ~Error() override = default;

   public:
    static Error InvalidArgument(std::string module, std::string message, std::string module_instance = {});
    static Error InvalidData(std::string module, std::string message, std::string module_instance = {});
    static Error NotFound(std::string module, std::string message, std::string module_instance = {});
    static Error OutOfRange(std::string module, std::string message, std::string module_instance = {});
    static Error Mismatch(std::string module, std::string message, std::string module_instance = {});
    static Error Overflow(std::string module, std::string message, std::string module_instance = {});
    static Error InvalidState(std::string module, std::string message, std::string module_instance = {});
    static Error Unsupported(std::string module, std::string message, std::string module_instance = {});
    static Error Io(std::string module, std::string message, std::string module_instance = {});
    static Error PathIo(std::string module, const std::filesystem::path& path, std::string_view action);

    const char* what() const noexcept override;
    Code GetCode() const noexcept;
    Type GetType() const noexcept;
    const std::string& GetModule() const noexcept;
    const std::string& GetModuleInstance() const noexcept;
    const std::string& GetMessage() const noexcept;

    static std::string_view CodeToString(Code code) noexcept;
    static std::string_view TypeToString(Type type) noexcept;

   private:
    static std::string BuildWhat(std::string_view module, std::string_view message);

   private:
    Code code_;
    std::string module_;
    std::string module_instance_;
    Type type_;
    std::string message_;
    std::string what_;
};
