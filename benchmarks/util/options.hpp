/*
 * GridTools
 *
 * Copyright (c) 2014-2020, ETH Zurich
 * All rights reserved.
 *
 * Please, refer to the LICENSE file in the root directory.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 */

// taken from GTBENCH
#pragma once

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

namespace ghex {
namespace options_impl {

void abort(std::string const &message) {
  std::cerr << "command line parsing error: " << message << std::endl;
  std::exit(1);
}

} // namespace options_impl

class options_values {
  template <class T> struct type {};

public:
  bool has(std::string const &name) const {
    return m_map.find(name) != m_map.end();
  }

  template <class T> T get(std::string const &name) const {
    auto value = m_map.find(name);
    if (value == m_map.end())
      options_impl::abort("no value found for option '" + name + "'");
    return parse(value->second, type<T>());
  }

  template <class T>
  T get_or(std::string const &name, T const &default_value) const {
    auto value = m_map.find(name);
    if (value == m_map.end())
      return default_value;
    return parse(value->second, type<T>());
  }

  operator bool() const { return !m_map.empty(); }

private:
  template <class T> static T parse_value(std::string const &value) {
    std::istringstream value_stream(value);
    T result;
    value_stream >> result;
    return result;
  }

  template <class T>
  static T parse(std::vector<std::string> const &values, type<T>) {
    if (values.size() != 1)
      options_impl::abort("wrong number of arguments requested");
    return parse_value<T>(values.front());
  }

  template <class T, std::size_t N>
  static std::array<T, N> parse(std::vector<std::string> const &values,
                                type<std::array<T, N>>) {
    if (values.size() != N)
      options_impl::abort("wrong number of arguments requested");
    std::array<T, N> result;
    for (std::size_t i = 0; i < N; ++i)
      result[i] = parse_value<T>(values[i]);
    return result;
  }

  std::map<std::string, std::vector<std::string>> m_map;

  friend class options;
};

class options {
  struct option {
    std::string name;
    std::string description;
    std::string variable;
    std::vector<std::string> default_values;
    std::size_t nargs;
  };

public:
  options_values parse(int argc, char **argv) const {
    options_values parsed;
    for (int i = 1; i < argc; ++i) {
      std::string arg(argv[i]);

      if (arg == "-h" || arg == "--help") {
        std::cout << help_message(argv[0]);
        std::exit(0);
      }

      if (arg[0] == '-' && arg[1] == '-') {
        std::string name = arg.substr(2);
        auto opt =
            std::find_if(m_options.begin(), m_options.end(),
                         [&](option const &o) { return o.name == name; });
        if (opt == m_options.end()) {
          options_impl::abort("unkown option: '" + arg + "'\n" +
                              help_message(argv[0]));
        }

        if (parsed.m_map.find(name) != parsed.m_map.end()) {
          options_impl::abort("multiple occurences of '" + arg + "'\n" +
                              help_message(argv[0]));
        }

        std::vector<std::string> values;
        for (std::size_t j = 0; j < opt->nargs; ++j, ++i) {
          if (i + 1 >= argc) {
            options_impl::abort(
                "unexpected end of arguments while parsing args for '" + arg +
                "'\n" + help_message(argv[0]));
          }
          std::string value(argv[i + 1]);
          if (value[0] == '-' && value[1] == '-')
            options_impl::abort("expected argument for option '" + arg +
                                "', found '" + value + "'\n" +
                                help_message(argv[0]));
          values.push_back(value);
        }

        parsed.m_map[name] = values;
      } else {
        options_impl::abort("unexpected token: '" + arg +
                            "' (too many arguments provided?)\n" +
                            help_message(argv[0]));
      }
    }

    for (auto const &opt : m_options) {
      if (!opt.default_values.empty() &&
          parsed.m_map.find(opt.name) == parsed.m_map.end()) {
        parsed.m_map[opt.name] = opt.default_values;
      }
    }

    return parsed;
  }

  options &operator()(std::string const &name, std::string const &description,
                      std::string const &variable, std::size_t nargs = 1) {
    return add(name, description, variable, {}, nargs);
  }

  template <class T>
  options &operator()(std::string const &name, std::string const &description,
                      std::string const &variable,
                      std::initializer_list<T> default_values) {
    std::vector<std::string> default_str_values;
    for (auto const &value : default_values) {
      std::ostringstream value_stream;
      value_stream << value;
      default_str_values.push_back(value_stream.str());
    }
    return add(name, description, variable, default_str_values,
               default_str_values.size());
  }

private:
  options &add(std::string const &name, std::string const &description,
               std::string const &variable,
               std::vector<std::string> const &default_values,
               std::size_t nargs) {
    assert(default_values.size() == 0 || default_values.size() == nargs);
    m_options.push_back({name, description, variable, default_values, nargs});
    return *this;
  }

  std::string help_message(std::string const &command) const {
    std::ostringstream out;
    out << "usage: " << command << " [options...]" << std::endl;

    std::size_t max_opt_len = 4;
    for (option const &opt : m_options) {
      max_opt_len =
          std::max(opt.name.size() + opt.variable.size(), max_opt_len);
    }

    auto print = [&out, max_opt_len](
                     std::string const &opt, std::string const &descr,
                     std::vector<std::string> const &default_values = {}) {
      out << "    " << std::left << std::setw(max_opt_len + 3) << opt << " "
          << descr;
      if (!default_values.empty()) {
        out << " (default:";
        for (auto const &default_value : default_values)
          out << " " << default_value;
        out << ")";
      }
      out << std::endl;
    };

    print("--help", "print this help message and exit");

    for (option const &opt : m_options) {
      print("--" + opt.name + " " + opt.variable, opt.description,
            opt.default_values);
    }

    return out.str();
  }

  std::vector<option> m_options;
};

} // namespace ghex
