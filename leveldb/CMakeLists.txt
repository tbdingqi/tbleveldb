# Copyright (c) 2006, 2010, Oracle and/or its affiliates. All rights reserved.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA

IF(WITH_DEBUG)
SET(dbg_flag "-d")
ELSE()
SET(dbg_flag "")
ENDIF()

ADD_CUSTOM_COMMAND(
  OUTPUT leveldb include
  PRE_BUILD
  COMMAND chmod +x build_leveldb
  COMMAND ./build_leveldb ${dbg_flag}
  COMMENT "leveldb built successfully"
)

ADD_CUSTOM_TARGET(ha_ldb ALL DEPENDS leveldb)

SET(LEVELDB_PLUGIN_DYNAMIC "ha_ldb")
SET(LEVELDB_SOURCES ha_ldb.cc)
INCLUDE_DIRECTORIES(include)
LINK_LIBRARIES(-Lleveldb libleveldb.a)
MYSQL_ADD_PLUGIN(ldb ${LEVELDB_SOURCES} STORAGE_ENGINE MODULE_ONLY)
