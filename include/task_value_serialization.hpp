#pragma once

#include "task_context.hpp"

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace task_serialization {

enum class ValueTag : std::uint8_t {
    Int = 1,
    Double = 2,
    String = 3,
    VectorInt = 4,
    VectorDouble = 5
};

inline void write_bytes(std::vector<std::uint8_t>& out, const void* data, std::size_t size) {
    const auto* ptr = static_cast<const std::uint8_t*>(data);
    out.insert(out.end(), ptr, ptr + size);
}

template <class T>
inline void write_pod(std::vector<std::uint8_t>& out, const T& value) {
    static_assert(std::is_trivially_copyable_v<T>, "write_pod requires trivially copyable type");
    write_bytes(out, &value, sizeof(T));
}

template <class T>
inline T read_pod(const std::vector<std::uint8_t>& data, std::size_t& offset) {
    static_assert(std::is_trivially_copyable_v<T>, "read_pod requires trivially copyable type");

    if (offset + sizeof(T) > data.size()) {
        throw std::runtime_error("read_pod: buffer underflow");
    }

    T value;
    std::memcpy(&value, data.data() + offset, sizeof(T));
    offset += sizeof(T);
    return value;
}

inline std::vector<std::uint8_t> serialize_value(const TaskContext::Value& value) {
    std::vector<std::uint8_t> out;

    std::visit([&](const auto& v) {
        using T = std::decay_t<decltype(v)>;

        if constexpr (std::is_same_v<T, int>) {
            write_pod(out, ValueTag::Int);
            write_pod(out, v);
        } else if constexpr (std::is_same_v<T, double>) {
            write_pod(out, ValueTag::Double);
            write_pod(out, v);
        } else if constexpr (std::is_same_v<T, std::string>) {
            write_pod(out, ValueTag::String);

            std::uint64_t size = static_cast<std::uint64_t>(v.size());
            write_pod(out, size);
            write_bytes(out, v.data(), v.size());
        } else if constexpr (std::is_same_v<T, std::vector<int>>) {
            write_pod(out, ValueTag::VectorInt);

            std::uint64_t size = static_cast<std::uint64_t>(v.size());
            write_pod(out, size);
            if (!v.empty()) {
                write_bytes(out, v.data(), v.size() * sizeof(int));
            }
        } else if constexpr (std::is_same_v<T, std::vector<double>>) {
            write_pod(out, ValueTag::VectorDouble);

            std::uint64_t size = static_cast<std::uint64_t>(v.size());
            write_pod(out, size);
            if (!v.empty()) {
                write_bytes(out, v.data(), v.size() * sizeof(double));
            }
        } else {
            static_assert(sizeof(T) == 0, "Unsupported TaskContext::Value type");
        }
    }, value);

    return out;
}

inline TaskContext::Value deserialize_value(const std::vector<std::uint8_t>& data) {
    std::size_t offset = 0;

    ValueTag tag = read_pod<ValueTag>(data, offset);

    switch (tag) {
        case ValueTag::Int: {
            int value = read_pod<int>(data, offset);
            return value;
        }

        case ValueTag::Double: {
            double value = read_pod<double>(data, offset);
            return value;
        }

        case ValueTag::String: {
            std::uint64_t size = read_pod<std::uint64_t>(data, offset);

            if (offset + size > data.size()) {
                throw std::runtime_error("deserialize_value: bad string size");
            }

            std::string value(
                reinterpret_cast<const char*>(data.data() + offset),
                static_cast<std::size_t>(size)
            );
            offset += static_cast<std::size_t>(size);
            return value;
        }

        case ValueTag::VectorInt: {
            std::uint64_t size = read_pod<std::uint64_t>(data, offset);
            std::size_t bytes = static_cast<std::size_t>(size) * sizeof(int);

            if (offset + bytes > data.size()) {
                throw std::runtime_error("deserialize_value: bad vector<int> size");
            }

            std::vector<int> value(static_cast<std::size_t>(size));
            if (size > 0) {
                std::memcpy(value.data(), data.data() + offset, bytes);
            }
            offset += bytes;
            return value;
        }

        case ValueTag::VectorDouble: {
            std::uint64_t size = read_pod<std::uint64_t>(data, offset);
            std::size_t bytes = static_cast<std::size_t>(size) * sizeof(double);

            if (offset + bytes > data.size()) {
                throw std::runtime_error("deserialize_value: bad vector<double> size");
            }

            std::vector<double> value(static_cast<std::size_t>(size));
            if (size > 0) {
                std::memcpy(value.data(), data.data() + offset, bytes);
            }
            offset += bytes;
            return value;
        }

        default:
            throw std::runtime_error("deserialize_value: unknown type tag");
    }
}

}