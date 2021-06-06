//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include <iostream>
#include "ldb_tool.h"

int main(int argc, char** argv) {
  LDBTool tool;
  std::string command;
  bool condition =true;
  while(condition)
  {
    std::getline(std::cin, command);
    std::size_t found = command.find("exit");
    if (found!=std::string::npos){
      //std::cout <<"Bye" <<std::endl;
      condition =false;
    }else{
      tool.RunCommand(command);
    }
  }
  return 0;
}