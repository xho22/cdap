/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import React, { Component, PropTypes } from 'react';
import 'whatwg-fetch';
import CaskVideo from 'components/CaskVideo';
require('./SplashScreen.less');

import Card from '../Card';
import MyUserStoreApi from '../../api/userstore';
import T from 'i18n-react';

 class SplashScreen extends Component {
  constructor(props) {
    super(props);
    this.props = props;
    this.state = {
      error: '',
      showSplashScreen: false,
      videoOpen: false
    };

    this.doNotShowCheck;
    this.onClose = this.onClose.bind(this);
    this.toggleVideo = this.toggleVideo.bind(this);
    this.toggleCheckbox = this.toggleCheckbox.bind(this);
  }

  componentWillMount() {
    MyUserStoreApi.get().subscribe((res) => {
      setTimeout(() => {
        this.setState({
          showSplashScreen : window.CDAP_CONFIG.cdap.standaloneWebsiteSDKDownload &&
            !window.CDAP_CONFIG.isEnterprise &&
            !res.property["user-choice-hide-welcome-message"]
        });
      }, 1000);
    });
  }

  //Handles the logic of whether or not to continue showing the splash screen to this user
  resetWelcomeMessage() {
    MyUserStoreApi
      .get()
      .flatMap(res => {
        res.property['user-choice-hide-welcome-message'] = this.doNotShowCheck;
        return MyUserStoreApi.set({}, res.property);
      })
      .subscribe(
        () => {},
        (err) => { this.setState({error: err}); }
      );
  }
  onClose() {
    this.setState({
      showSplashScreen: false
    });
    this.resetWelcomeMessage();
  }
  toggleVideo(){
    this.setState({
      videoOpen : !this.state.videoOpen
    });
  }
  closeVideo(){
    if(this.state.videoOpen){
      this.setState({
        videoOpen: false
      });
    }
  }
  toggleCheckbox() {
    this.doNotShowCheck = !this.doNotShowCheck;
  }
  render() {

    let cardTitle = this.state.videoOpen ? '' : T.translate('features.SplashScreen.title');
    let cardTitleTwo = this.state.videoOpen ? '' : T.translate('features.SplashScreen.titleTwo');

    let cardHeader = (
      <div className="card-header-splash">
        <h3>{cardTitle}</h3>
        <h3>{cardTitleTwo}</h3>
        <span
          className="fa fa-times"
          onClick={this.onClose}
        />
      </div>
    );

    return (
      <div className={!this.state.showSplashScreen ? 'hide' : ''}>
        <div className="splash-screen-backdrop"></div>
        <div className="splash-screen">
          <Card
            className="splash-screen-card"
            closeable
            header={cardHeader}
          >
            <div className="text-center">
              <div className="splash-main-container">
              {
                this.state.videoOpen ?
                  <div className="cask-video-container">
                    <CaskVideo />
                  </div>
                :
                  <div>
                    <img width="100px" src="cdap_assets/img/cdaplogo_white.png" />
                    <div className="version-label">
                      {T.translate('features.SplashScreen.version-label')}
                    </div>
                    <h4>
                      {T.translate('features.SplashScreen.intro-message')}
                    </h4>
                  </div>
              }
              </div>
              <br />
              <div className={'group'}>
                <a className="spash-screen-btn" target="_blank" href="http://docs.cask.co/cdap">
                  <div className="btn btn-default">
                    <span className="fa fa-book btn-icon"></span>{T.translate('features.SplashScreen.buttons.getStarted')}
                  </div>
                </a>
                <div
                  className={'btn btn-default spash-screen-btn'}
                  onClick={this.toggleVideo}
                >
                  <span className="fa fa-youtube-play btn-icon"></span>{T.translate('features.SplashScreen.buttons.introduction')}
                </div>
                <a target="_blank" href="http://cask.co/company/contact/#mailing-list">
                  <div
                    className={'btn btn-default spash-screen-btn'}
                  >
                    <span className="fa fa-pencil-square btn-icon" />{"Get Updates"}
                  </div>
                </a>
              </div>
              <div className="splash-checkbox">
                <input onChange={this.toggleCheckbox} type="checkbox" />
                <span className="splash-checkbox-label"> {T.translate('features.SplashScreen.dontShow')} </span>
              </div>
            </div>
          </Card>
        </div>
      </div>
    );
  }
}

const propTypes = {
  openVideo: PropTypes.func
};

SplashScreen.propTypes = propTypes;
export default SplashScreen;
