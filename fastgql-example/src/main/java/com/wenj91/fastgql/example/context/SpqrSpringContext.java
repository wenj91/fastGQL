package com.wenj91.fastgql.example.context;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class SpqrSpringContext implements ApplicationContextAware {

  private ApplicationContext applicationContext;

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.applicationContext = applicationContext;
  }

  /**
   * 获取applicationContext对象
   *
   * @return
   */
  public ApplicationContext getApplicationContext() {
    return applicationContext;
  }
}
