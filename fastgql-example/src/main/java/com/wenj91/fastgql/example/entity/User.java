package com.wenj91.fastgql.example.entity;


import com.wenj91.fastgql.annotation.Permissions;
import com.wenj91.fastgql.annotation.AuthGroup;
import com.wenj91.fastgql.annotation.Referencing;
import lombok.Data;

import javax.persistence.*;
import java.util.Date;

@Data
@Entity
@Table(name = "t_user")
@AuthGroup(permissions = {
    @Permissions(name = "User", value = "query"),
    @Permissions(name = "User_by_pk", value = "query"),
    @Permissions(name = "User_aggregate", value = "update"),
    @Permissions(name = "User_insert", value = "update"),
})
public class User {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "id")
  private Integer id;

  @Column(name = "name")
  private String name;

  @Column(name = "password")
  private String password;

  @Column(name = "create_time")
  private Date createTime;

  @Column(name = "test_id")
  @Referencing(refEntity = "Test", refField = "id")
  private Integer testId;
}
