package com.geekbang.recommend.service;
import com.geekbang.recommend.dao.UserRepository;
import com.geekbang.recommend.entity.UserEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.ui.ModelMap;
import javax.annotation.Resource;

@Service
public class UserService {

    @Autowired
    private UserRepository userRepository;
    @Resource
    private BCryptPasswordEncoder bCryptPasswordEncoder;

    public UserEntity add(UserEntity userEntity) {
        userEntity.setPassword(bCryptPasswordEncoder.encode(userEntity.getPassword()));
        UserEntity userEntity1 = userRepository.save(userEntity);
        return userEntity1;
    }

    public ModelMap login(UserEntity userEntity) {
        ModelMap modelMap = new ModelMap();
        UserEntity userEntity2 = userRepository.getUserByName(userEntity.getName());
        if(userEntity2 == null) {
            modelMap.addAttribute("result", false);
            modelMap.addAttribute("msg", "用户不存在");
            return modelMap;
        }
        if(!bCryptPasswordEncoder.matches(userEntity.getPassword(), userEntity2.getPassword())) {
            modelMap.addAttribute("result", false);
            modelMap.addAttribute("msg", "密码错误");
            return modelMap;
        }
        modelMap.addAttribute("result", true);
        modelMap.addAttribute("user", userEntity2);
        return modelMap;
    }

    public UserEntity findByName(String name) {
        UserEntity userEntity2 = userRepository.getUserByName(name);
        if(userEntity2 == null) {
            return null;
        }
        return userEntity2;
    }
}
