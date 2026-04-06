#pragma once

#include <any>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>

class TaskContext {
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::any> data_;
public:
    TaskContext() = default;

    template <class T>
    void set(const std::string& key, T value) {
        std::lock_guard<std::mutex> lock(mutex_);
        data_[key] = std::any(std::move(value));
    }

    template <class T>
    T get_copy(const std::string& key) const {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = data_.find(key);
        if (it == data_.end()) {
            throw std::runtime_error("TaskContext: key not found: " + key);
        }

        const T* ptr = std::any_cast<T>(&it->second);
        if (!ptr) {
            throw std::runtime_error("TaskContext: bad type for key: " + key);
        }

        return *ptr;
    }

    template <class T>
    void update(const std::string& key, T value) {
        set<T>(key, std::move(value));
    }

    bool contains(const std::string& key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return data_.find(key) != data_.end();
    }

};