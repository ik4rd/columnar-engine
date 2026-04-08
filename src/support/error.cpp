#include "error.h"

#include <utility>

std::string Error::BuildWhat(const std::string_view module, const std::string_view message) {
    return std::string(module) + ": " + std::string(message);
}

Error::Error(const Code code, std::string module, std::string module_instance, const Type type, std::string message)
    : code_(code),
      module_(std::move(module)),
      module_instance_(std::move(module_instance)),
      type_(type),
      message_(std::move(message)),
      what_(BuildWhat(module_, message_)) {}

Error Error::InvalidArgument(std::string module, std::string message, std::string module_instance) {
    return Error(Code::InvalidArgument, std::move(module), std::move(module_instance), Type::Validation,
                 std::move(message));
}

Error Error::InvalidData(std::string module, std::string message, std::string module_instance) {
    return Error(Code::InvalidData, std::move(module), std::move(module_instance), Type::Data, std::move(message));
}

Error Error::NotFound(std::string module, std::string message, std::string module_instance) {
    return Error(Code::NotFound, std::move(module), std::move(module_instance), Type::State, std::move(message));
}

Error Error::OutOfRange(std::string module, std::string message, std::string module_instance) {
    return Error(Code::OutOfRange, std::move(module), std::move(module_instance), Type::Validation, std::move(message));
}

Error Error::Mismatch(std::string module, std::string message, std::string module_instance) {
    return Error(Code::Mismatch, std::move(module), std::move(module_instance), Type::Validation, std::move(message));
}

Error Error::Overflow(std::string module, std::string message, std::string module_instance) {
    return Error(Code::Overflow, std::move(module), std::move(module_instance), Type::Validation, std::move(message));
}

Error Error::InvalidState(std::string module, std::string message, std::string module_instance) {
    return Error(Code::InvalidState, std::move(module), std::move(module_instance), Type::State, std::move(message));
}

Error Error::Unsupported(std::string module, std::string message, std::string module_instance) {
    return Error(Code::Unsupported, std::move(module), std::move(module_instance), Type::Validation,
                 std::move(message));
}

Error Error::Io(std::string module, std::string message, std::string module_instance) {
    return Error(Code::IoFailure, std::move(module), std::move(module_instance), Type::Io, std::move(message));
}

Error Error::PathIo(std::string module, const std::filesystem::path& path, const std::string_view action) {
    return Io(std::move(module), "failed to " + std::string(action) + " '" + path.string() + "'", path.string());
}

const char* Error::what() const noexcept { return what_.c_str(); }

Error::Code Error::GetCode() const noexcept { return code_; }

Error::Type Error::GetType() const noexcept { return type_; }

const std::string& Error::GetModule() const noexcept { return module_; }

const std::string& Error::GetModuleInstance() const noexcept { return module_instance_; }

const std::string& Error::GetMessage() const noexcept { return message_; }

std::string_view Error::CodeToString(const Code code) noexcept {
    switch (code) {
        case Code::InvalidArgument:
            return "invalid_argument";
        case Code::InvalidData:
            return "invalid_data";
        case Code::NotFound:
            return "not_found";
        case Code::OutOfRange:
            return "out_of_range";
        case Code::Mismatch:
            return "mismatch";
        case Code::Overflow:
            return "overflow";
        case Code::InvalidState:
            return "invalid_state";
        case Code::Unsupported:
            return "unsupported";
        case Code::IoFailure:
            return "io_failure";
    }
    return "unknown";
}

std::string_view Error::TypeToString(const Type type) noexcept {
    switch (type) {
        case Type::Validation:
            return "validation";
        case Type::Data:
            return "data";
        case Type::State:
            return "state";
        case Type::Io:
            return "io";
    }
    return "unknown";
}
