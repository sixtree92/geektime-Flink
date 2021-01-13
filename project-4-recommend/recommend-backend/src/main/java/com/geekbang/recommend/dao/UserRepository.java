package com.geekbang.recommend.dao;

import com.geekbang.recommend.entity.UserEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

public interface UserRepository extends JpaRepository<UserEntity, String>, JpaSpecificationExecutor<UserEntity> {

    UserEntity getUserByName(String name);

    UserEntity save(UserEntity userEntity);
}
